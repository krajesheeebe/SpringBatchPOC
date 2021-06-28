package com.rajesh.batchApp.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

import com.rajesh.batchApp.dto.AccountDetails;

public class CSVDataSkipListener implements SkipListener<AccountDetails, AccountDetails> {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void onSkipInRead(Throwable throwable) {
		logger.warn("Line skipped on read", throwable);
	}

	@Override
	public void onSkipInWrite(AccountDetails bankTransaction, Throwable throwable) {
		logger.warn("Bean skipped on write", throwable);
	}

	@Override
	public void onSkipInProcess(AccountDetails bankTransaction, Throwable throwable) {
		logger.warn("Bean skipped on process", throwable);
	}

}