package io.iguaz.examples.utils;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.iguaz.v3io.daemon.client.api.consts.V3IOResultCode.Errors;
import io.iguaz.v3io.streaming.api.V3IOPutRecordCallback;

public final class PutRecordsCallback implements V3IOPutRecordCallback {
    private static final Logger log = LoggerFactory.getLogger(io.iguaz.v3io.container.streaming.AccumulativePutRecordCallback.class);
    public static final long DEFAULT_TIMEOUT_MS = 1000L;
    private AtomicInteger successResponses = new AtomicInteger(0);
    private AtomicInteger failureResponses = new AtomicInteger(0);

    public PutRecordsCallback() {
    }

    public void onSuccess(long var1, short var3) {
        this.successResponses.getAndIncrement();
        log.trace("Put record [partition-id:{}/sequence-id:{}] succeed", var3, var1);
    }

    public void onFailure(Errors var1) {
        this.failureResponses.getAndIncrement();
        log.trace("Put record failed with error code: {}", var1);
    }

    private long getResponsesCount() {
        return (long) (this.successResponses.get() + this.failureResponses.get());
    }

    public void waitForResponses(long recordCount, long timeout) {
        int time_elapsed = 0;

        while ((long) time_elapsed < timeout && recordCount != this.getResponsesCount()) {
            try {
                Thread.sleep(50L);
                time_elapsed += 50;
                if (time_elapsed % 60000 == 0) {
                    log.info("Responses: Success = " + this.successResponses.get() +
                            " Failures = " + this.failureResponses.get());
                }
            } catch (InterruptedException var7) {
                log.error("Waiting thread was interrupted before got responses");
            }
        }

        if (recordCount != this.getResponsesCount()) {
            String error_msg = String.format("Put records did not finish on time.%n\t Expected to receive %d responses%n\tSuccessfully published %d records%n\tFailed publishing %d records", recordCount, this.successResponses.get(), this.failureResponses.get());
            throw new AssertionError(error_msg);
        }
        if (this.failureResponses.get() > 0) {
            throw new AssertionError(String.format("%d putRecord operations failed", this.failureResponses.get()));
        }
    }
}
