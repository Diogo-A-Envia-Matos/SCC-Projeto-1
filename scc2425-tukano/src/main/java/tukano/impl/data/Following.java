package tukano.impl.data;

import java.util.Objects;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class Following{

	@Id
	String id;

	String follower;
	
	String followee;

	Following() {}

	public Following(String follower, String followee) {
		this.follower = follower;
		this.followee = followee;
		this.id = buildId(follower, followee);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getFollower() {
		return follower;
	}

	public void setFollower(String follower) {
		this.follower = follower;
	}

	public String getFollowee() {
		return followee;
	}

	public void setFollowee(String followee) {
		this.followee = followee;
	}

	@Override
	public int hashCode() {
		return Objects.hash(followee, follower);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Following other = (Following) obj;
		return Objects.equals(followee, other.followee) && Objects.equals(follower, other.follower);
	}

	@Override
	public String toString() {
		return "Following [follower=" + follower + ", followee=" + followee + "]";
	}

	public static String buildId(String follower, String followee) {
		return String.format("%s-%s", follower, followee);
	}
}