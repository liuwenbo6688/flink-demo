package com.datax.phd.model;



import java.util.ArrayList;
import java.util.List;

/**
 *
 */

public class UserEventContainer {
    private String userId;
    private List<UserEvent> userEvents=new ArrayList<>(); //某个用户的所有事件


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public List<UserEvent> getUserEvents() {
        return userEvents;
    }

    public void setUserEvents(List<UserEvent> userEvents) {
        this.userEvents = userEvents;
    }


    @Override
    public String toString() {
        return "UserEventContainer{" +
                "userId='" + userId + '\'' +
                ", userEvents=" + userEvents +
                '}';
    }
}
