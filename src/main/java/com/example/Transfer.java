package com.example;


import java.util.Date;

public class Transfer {
    private String station;
    private Date startDate;
    private Date endDate;
    int noOfDays;
    int rowNumber;

    public Transfer(String station, Date startDate, Date endDate, int noOfDays) {
        this.station = station;
        this.startDate = startDate;
        this.endDate = endDate;
        this.noOfDays= noOfDays;
    }

    public String getStation() {
        return station;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

public int getNoOfDays() {
return noOfDays;
}

public int getRowNumber() {
return rowNumber;
}

}