package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

import java.rmi.server.LogStream;
import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null || lockType == LockType.NL) {
            return;
        }
        if (lockContext.getEffectiveLockType(transaction) == LockType.X) {
            return;
        }
        if (lockContext.getEffectiveLockType(transaction) == lockType && lockType == LockType.S) {
            return;
        }
        if (lockContext.parentContext() != null) {
            checkAncestors(transaction, lockContext.parentContext(), lockType);
        }
        acquireLock(transaction, lockContext, lockType);

    }

    // TODO(proj4_part2): add helper methods as you see fit
    public static void checkAncestors(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        if (lockContext.parentContext() != null) {
            checkAncestors(transaction, lockContext.parentContext(), lockType);
        }

        if (lockType == LockType.S) {
            if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
                lockContext.acquire(transaction, LockType.IS);
            }
        } else if (lockType == LockType.X) {
            if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
                lockContext.acquire(transaction, LockType.IX);
            } else if (lockContext.getExplicitLockType(transaction) == LockType.IS){
                lockContext.promote(transaction, LockType.IX);
            }
        }
    }

    public static void acquireLock(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        if (lockType == LockType.S) {
            if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
                lockContext.acquire(transaction, lockType);
            } else if (lockContext.getExplicitLockType(transaction) == LockType.IX) {
                lockContext.promote(transaction, LockType.SIX);
            } else if (lockContext.getExplicitLockType(transaction) == LockType.IS) {
                lockContext.escalate(transaction);
            }
        } else if (lockType == LockType.X) {
            if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
                lockContext.acquire(transaction, lockType);
            } else if (lockContext.getExplicitLockType(transaction) == LockType.S){
                lockContext.escalate(transaction);
                lockContext.promote(transaction, lockType);
            }
        }
    }
}