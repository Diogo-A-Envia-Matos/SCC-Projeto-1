package utils;

import exceptions.InvalidClassException;
import tukano.api.User;
import tukano.api.Short;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;

public class GetId {

    public static String getId(Object obj) {
        if (obj instanceof User) {
            return ((User) obj).getId();
        }
        else if (obj instanceof Short) {
            return ((Short) obj).getId();
        }
        else if (obj instanceof Following) {
            return String.format("%s-%s", ((Following) obj).getFollower(), ((Following) obj).getFollowee());
        }
        else if (obj instanceof Likes) {
            return String.format("%s-%s", ((Likes) obj).getUserId(), ((Likes) obj).getShortId());
        }
        else {
			throw new InvalidClassException("Invalid Class: " + obj.getClass().toString());
        }
    }
}
