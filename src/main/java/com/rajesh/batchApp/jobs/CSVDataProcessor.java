package com.rajesh.batchApp.jobs;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.rajesh.batchApp.dto.AccountDetails;

@Component
@StepScope
public class CSVDataProcessor implements ItemProcessor<AccountDetails, AccountDetails> {

	@Override
	public AccountDetails process(AccountDetails accountDetails) throws IllegalArgumentException {
//      to check for any undesired data
//		if ("".equals(accountDetails.getClusterType())) {
//			throw new IllegalArgumentException("Cluster Type is empty.");
//		}

		return accountDetails;
	}
}
