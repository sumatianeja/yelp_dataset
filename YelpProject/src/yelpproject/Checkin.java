/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpproject;

import java.util.List;

/**
 *
 * @author sumati
 */
public class Checkin {

    private String business_id;
    private String type;
    private List<String> checkin_info;
    
    public String getBusiness_id() {
        return business_id;
    }

    public void setBusiness_id(String business_id) {
        this.business_id = business_id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getCheckin_info() {
        return checkin_info;
    }

    public void setCheckin_info(List<String> checkin_info) {
        this.checkin_info = checkin_info;
    }
    
}
