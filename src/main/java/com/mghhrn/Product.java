package com.mghhrn;

import io.vertx.core.json.JsonObject;

public class Product {

    private Long id;
    private String name;
    private Long price;

    public Product() {}

    public Product(JsonObject jsonObject) {
        this.id = jsonObject.getLong("id");
        this.name = jsonObject.getString("name");
        this.price = jsonObject.getLong("price");
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }
}
