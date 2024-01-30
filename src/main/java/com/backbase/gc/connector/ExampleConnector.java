package com.backbase.gc.connector;

// camel-k: language=java
// camel-k: name=mambu-loan-transaction-connector

import com.backbase.grandcentralgeneric.constants.ErrorConstants;
import com.backbase.grandcentralgeneric.exception.GrandCentralServiceException;
import com.backbase.grandcentralgeneric.processor.NoRecordsProcessor;
import com.backbase.grandcentralgeneric.util.GrandCentralUtil;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.http.base.HttpOperationFailedException;


@SuppressWarnings("unused")
public class ExampleConnector extends RouteBuilder {

    @Override
    public void configure() {

        onException(HttpOperationFailedException.class)
                .handled(true)
                .to("kamelet:http-exception-handler?errorTemplate=transform-mambu-error-details-v2.json");

        getContext().getRegistry().bind("grandCentralUtil", new GrandCentralUtil());

        onException(GrandCentralServiceException.class)
                .handled(true)
                .to("kamelet:grand-central-service-exception-handler");

        onException(Exception.class)
                .handled(true)
                .to("kamelet:generic-exception-handler");

        //Get Account Transactions - Retrieve and Transform
        from("direct://getLoanTransactionsByLoanId")
                .log(LoggingLevel.INFO, "Initiating getLoanTransactionsByLoanId route")

                .setProperty("queryString", constant("detailsLevel=FULL"))
                .setProperty("additionalMessage", simple("${header.loanId}"))
                .to("kamelet:mambu-api-caller?apiUri=loans/${header.loanId}&requiresQueryString=true&retry=true&httpMethod=GET")
                .log(LoggingLevel.DEBUG, "parent account details: ${body}")
                .convertBodyTo(String.class)
                .setProperty("parentAccountDetails", simple("${body}"))

                .bean(GrandCentralUtil.class, "convertHeadersToJsonBody(${exchange}, 'fromDate:toDate:loanId')")
                .to("jolt:get-loan-transactions-request-transformation.json?transformDsl=Chainr&inputType=JsonString&outputType=JsonString")

                .log(LoggingLevel.DEBUG, "Invoking mambu to get transactions with loan id ${header.loanId}")
                .setProperty("queryString", constant("detailsLevel=FULL"))
                .to("kamelet:mambu-api-caller?apiUri=/loans/transactions:search&requiresQueryString=true&retry=true&httpMethod=POST")
                .log(LoggingLevel.DEBUG, "Search Loan Transactions response from mambu : ${body}")

                .process(new NoRecordsProcessor(ErrorConstants.NO_TRANSACTIONS_FOUND_FOR_ACCOUNT_ERROR_CODE))

                .to("direct://addParentAccountDetails")
                .to("direct://addLinkedAccountDetails")

                .log(LoggingLevel.DEBUG, " Final Transactions:  ${body}")
                .to("jolt:get-loan-transactions-response-transformation.json?transformDsl=Chainr&inputType=JsonString&outputType=JsonString")
                .log(LoggingLevel.INFO, "Completed getLoanTransactionsByLoanId process");

        from("direct://addParentAccountDetails")
                .split(jsonpath("$.[*]"), AggregationStrategies.groupedBody()).parallelProcessing().stopOnException()
                .bean(GrandCentralUtil.class, "appendJsonNode(${body}, 'parentAccountDetails', ${exchangeProperty.parentAccountDetails})")
                .log(LoggingLevel.DEBUG, "Transaction Details:  ${body}")
                .end()
                .setBody().groovy("request.body.stream().collect([], {it})")
                .marshal().json()
                .log(LoggingLevel.DEBUG, "ParentAccountDetails added : ${body}");

        from("direct://addLinkedAccountDetails")
                .unmarshal().json()
                .setProperty("otherTransactions").groovy("request.body.findAll{ !it.containsKey('transferDetails') || (!it.transferDetails?.containsKey('linkedDepositTransactionKey') && !it.transferDetails?.containsKey('linkedLoanTransactionKey'))}")
                .multicast().aggregationStrategy(AggregationStrategies.groupedBody()).parallelProcessing().stopOnException()
                .to("direct:getLinkedDepositAccountDetails", "direct:getLinkedLoanAccountDetails")
                .end()
                .setBody().groovy("request.body.stream().collect([], {it}).flatten()")
                .setBody().groovy("request.body.addAll(exchange.properties.otherTransactions); return request.body;")
                .marshal().json();

        from("direct:getLinkedDepositAccountDetails")
                .setBody().groovy("request.body.findAll{ it.transferDetails?.containsKey('linkedDepositTransactionKey')}")
                .choice().when(simple("${body.size} > 0"))
                .setProperty("depositAccountTransactions", body())
                .to("jolt:get-linked-transaction-request-transformation.json?transformDsl=Chainr&outputType=JsonString")
                .setProperty("queryString", constant("detailsLevel=BASIC"))
                .to("kamelet:mambu-api-caller?apiUri=/deposits/transactions:search&requiresQueryString=true&retry=true&httpMethod=POST")
                .unmarshal().json()
                .setBody().groovy("exchange.properties.depositAccountTransactions.each{ transaction -> transaction['transferAccountKey'] = request.body.find{ linkedTransaction -> linkedTransaction['encodedKey'] == transaction.transferDetails['linkedDepositTransactionKey']}.parentAccountKey}")

                .setProperty("depositAccountTransactions", body())
                .to("jolt:get-linked-transaction-accounts-request-transformation.json?transformDsl=Chainr&outputType=JsonString")
                .setProperty("queryString", constant("detailsLevel=FULL"))
                .to("kamelet:mambu-api-caller?apiUri=/deposits:search&requiresQueryString=true&retry=true&httpMethod=POST")
                .unmarshal().json()
                .setBody().groovy("exchange.properties.depositAccountTransactions.each{ transaction -> transaction['transferAccountDetails'] = request.body.find{ account -> account['encodedKey'] == transaction['transferAccountKey']}}")
                .end()
                .log(LoggingLevel.DEBUG, "LinkedDepositAccountDetails Transactions : ${body}");

        from("direct:getLinkedLoanAccountDetails")
                .setBody().groovy("request.body.findAll{ it.transferDetails?.containsKey('linkedLoanTransactionKey')}")
                .choice().when(simple("${body.size} > 0"))
                .setProperty("loanAccountTransactions", body())
                .to("jolt:get-linked-transaction-request-transformation.json?transformDsl=Chainr&outputType=JsonString")
                .setProperty("queryString", constant("detailsLevel=BASIC"))
                .to("kamelet:mambu-api-caller?apiUri=/loans/transactions:search&requiresQueryString=true&retry=true&httpMethod=POST")
                .unmarshal().json()
                .setBody().groovy("exchange.properties.loanAccountTransactions.each{ transaction -> transaction['transferAccountKey'] = request.body.find{ linkedTransaction -> linkedTransaction['encodedKey'] == transaction.transferDetails['linkedLoanTransactionKey']}.parentAccountKey}")

                .setProperty("loanAccountTransactions", body())
                .to("jolt:get-linked-transaction-accounts-request-transformation.json?transformDsl=Chainr&outputType=JsonString")
                .setProperty("queryString", constant("detailsLevel=FULL"))
                .to("kamelet:mambu-api-caller?apiUri=/loans:search&requiresQueryString=true&retry=true&httpMethod=POST")
                .unmarshal().json()
                .setBody().groovy("exchange.properties.loanAccountTransactions.each{ transaction ->  transaction['transferAccountDetails'] = request.body.find{ account -> account['encodedKey'] == transaction['transferAccountKey']}}")
                .end()
                .log(LoggingLevel.DEBUG, "LinkedLoanAccountDetails Transactions : ${body}");

        //Get Transaction - Retrieve and Transform
        from("direct://getLoanTransactionByTransactionId")
                .log(LoggingLevel.INFO, "Initiating getLoanTransactionByTransactionId route")

                .log(LoggingLevel.DEBUG, "Invoking mambu to get transaction with id ${header.transactionId}")
                .setProperty("queryString", constant("detailsLevel=FULL"))
                .to("kamelet:mambu-api-caller?apiUri=/loans/transactions/${headers.transactionId}&requiresQueryString=true&retry=true&httpMethod=GET")
                .log(LoggingLevel.DEBUG, "Received response from mambu : ${body}")
                .setProperty("transactionDetails", simple("${body}"))

                .setProperty("parentAccountKey", jsonpath("$.parentAccountKey"))
                .log(LoggingLevel.DEBUG, "Invoking mambu to get parent account details for id: ${exchangeProperty.parentAccountKey}")
                .setProperty("queryString", constant("detailsLevel=FULL"))
                .to("kamelet:mambu-api-caller?apiUri=loans/${exchangeProperty.parentAccountKey}&requiresQueryString=true&retry=true&httpMethod=GET")
                .log(LoggingLevel.DEBUG, "Received response from mambu : ${body}")
                .setProperty("parentAccountDetails", simple("${body}"))
                .setBody(simple("${exchangeProperty.transactionDetails}"))
                .bean(GrandCentralUtil.class, "appendJsonNode(${body}, 'parentAccountDetails', ${exchangeProperty.parentAccountDetails})")

                .setBody().groovy("request.body = [request.body].toList()")
                .marshal().json()

                .to("direct://addLinkedAccountDetails")

                .to("jolt:get-transaction-by-id-response-transformation.json?transformDsl=Chainr&inputType=JsonString&outputType=JsonString")
                .log(LoggingLevel.DEBUG, "Get transactions by id response :  ${body}")
                .log(LoggingLevel.INFO, "Completed getLoanTransactionByTransactionId process");

        //Get Multiple Loan Account Transactions - Split, Retrieve, Transform, Aggregate
        from("direct://getLoanTransactionsForMultipleLoans")
                .log(LoggingLevel.INFO, "Initiating getLoanTransactionsForMultipleLoans route")
                .setBody().groovy("request.headers.loanIds instanceof String ? request.headers.loanIds.tokenize(',').unique() : request.headers.loanIds.unique()")

                .split(body(), AggregationStrategies.groupedBody()).parallelProcessing().stopOnException()
                .setProperty("loanId", simple("${body}"))
                .setProperty("additionalMessage", body())
                .log(LoggingLevel.DEBUG, "Invoking mambu to retrieve loan account transactions for loanId: ${body}")

                .setProperty("queryString", constant("detailsLevel=FULL"))
                .to("kamelet:mambu-api-caller?apiUri=loans/${exchangeProperty.loanId}&requiresQueryString=true&retry=true&httpMethod=GET")
                .convertBodyTo(String.class)
                .setProperty("parentAccountDetails", simple("${body}"))

                .setProperty("queryString", constant("detailsLevel=FULL"))
                .to("kamelet:mambu-api-caller?apiUri=loans/${exchangeProperty.loanId}/transactions/&requiresQueryString=true&retry=true&httpMethod=GET")

                .to("direct://addParentAccountDetails")
                .to("direct://addLinkedAccountDetails")

                .log(LoggingLevel.DEBUG, "Received loan transactions data from mambu for ${exchangeProperty.loanId} : ${body}")

                .transform().simple("{\"transactions\":${body}}").unmarshal().json()
                .setBody().groovy("request.body.parentAccountId = exchange.properties.loanId; return request.body")
                .log(LoggingLevel.DEBUG, "Modified body with addition of loanId for ${exchangeProperty.loanId} : ${body}")

                .marshal().json()
                .to("jolt:get-transactions-by-loan-ids-response-transformation.json?transformDsl=Chainr&inputType=JsonString&outputType=JsonString")
                .log(LoggingLevel.DEBUG, "Transformed JOLT response for ${exchangeProperty.loanId} : ${body}")
                .unmarshal().json()
                .end()
                .setBody().groovy("request.body.stream().collect([], {it}).flatten()")
                .marshal().json()
                .log(LoggingLevel.DEBUG, "Get loan accounts' transactions response:  ${body}")
                .log(LoggingLevel.INFO, "Completed getLoanTransactionsForMultipleLoans process");
    }
}