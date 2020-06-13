package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        boolean b_is_ix = b == IS || b == IX;

        if ((a == NL || b == NL) ||
            (a == SIX && b == IS) ||
            (a == S && (b == IS || b == S)) ||
            (a == IX && b_is_ix) ||
            (a == IS && (b_is_ix || b == S || b == SIX))) {
            return true;
        }
        
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    public static boolean checkLockExists(LockType lock) {
        switch (lock) {
            case S: return true;
            case X: return true;
            case IS: return true;
            case IX: return true;
            case SIX: return true;
            case NL: return true;
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        if (childLockType == NL){
            return true;
        }

        checkLockExists(parentLockType);
        checkLockExists(childLockType);

        if (parentLockType == IX ||
            (parentLockType == SIX && (childLockType == IX || childLockType == X || childLockType == SIX)) ||
            (parentLockType == IS && (childLockType == IS || childLockType == S))) {
            return true;
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        boolean sub_x_six = substitute == X || substitute == SIX;

        if ((required == NL) ||
            (required == substitute) ||
            (required == S && sub_x_six) ||
            (required == IX && sub_x_six) ||
            (required == IS && (substitute == IX || substitute == SIX))) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

