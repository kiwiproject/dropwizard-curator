package org.kiwiproject.curator.exception;

import static org.kiwiproject.curator.test.util.StandardExceptionTests.standardConstructorTestsFor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Collection;

@DisplayName("LockAcquisitionTimeoutException")
class LockAcquisitionTimeoutExceptionTest {

    @TestFactory
    Collection<DynamicTest> shouldHaveStandardConstructors() {
        return standardConstructorTestsFor(LockAcquisitionTimeoutException.class);
    }
}
