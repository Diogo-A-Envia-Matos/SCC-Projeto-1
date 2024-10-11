package utils;

import tukano.api.Result;
import tukano.api.Result.ErrorCode;
import tukano.api.User;
import tukano.api.Short;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;

public class GetId {

    public static String getId(Object obj) {
        if (obj instanceof User) {
            return ((User) obj).getUserId();
        }
        else if (obj instanceof Short) {
            return ((Short) obj).getShortId();
        }
        else if (obj instanceof Following) {
            return ((Following) obj).getFollower();
        }
        else if (obj instanceof Likes) {
            return ((Likes) obj).getUserId();
        }
        else {
            return null;
        }
    }
}
