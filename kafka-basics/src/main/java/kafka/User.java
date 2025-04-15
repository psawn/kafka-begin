package kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class User {
    @NotNull
    @JsonProperty
    public String id;

    public User() {
    }

    public User(String id) {
        this.id = id;
    }
}