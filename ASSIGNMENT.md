### Assignment

The goal of this assignment is to provide the Network department of KLM with 
valuable insights into the most popular destinations in their network. The 
Network department is looking to add some new flight routes to KLMs 
ever-expanding list of destinations. To this they would first like to 
know which countries are the most popular per season, for each day of the 
week, over a given time period.

To achieve this, we have the booking data available to use,
which shows on which flights passengers have booked. We also have an 
opensource dataset which shows in which country each airport is located.
Using these datasets build a solution that can generate this report for 
the business. 

### Functional Requirements

 * The solution should be able to run on a batch of data. The example data sets
   are pretty small, but the real-life data sets can have sizes of up to 100s of TBs.
 * The user should be able to specify a local directory or a HDFS URI as input for 
   the bookings (You can assume all the required configuration to connect to HDFS 
   is stored in /etc/hadoop/conf/)
 * The user should be able to specify a start-date and end-date, and the solution
   should only calculate the top destination countries based on bookings within this
   range.
 * We are currently only interested in KL flights departing from the Netherlands.
 * The day of the week should be based on the local timezone for the airport.
 * Each passenger should only be counted once per flight leg.
 * We should only count a passenger if their latest booking status is Confirmed.
 * The output should be a table where each row shows the number of passengers 
   per country, per day of the week, per season. The table should be sorted in 
   descending order by the number of bookings, grouped by season and day of the week.

#### Bonus objectives

**Note: These are extra objectives you can try to complete if you desire, they are not necessary**

 * Add more information per country, how many adults vs. children for example, 
   
 * Have the ability to run on a stream of realtime data, showing the top country
   for the day. 

### Non Functional Requirements

 * The given booking dataset is just a sample to show the format of the input data,
   your solution should be able to take as input a directory location on HDFS,
   containing many files in the same format totaling TBs in size.
 * The most optimal solution is desired, so that the job takes the least amount
   of time as possible, while still being elegant and easily understood by
   fellow developers.
 * The solution should be runnable on a laptop for a small dataset, as well as 
   have the ability to be deployed onto a Hadoop YARN cluster to handle larger datasets.
   
      
### Deliverables

This assignment should be delivered in the following way:

 * All code is pushed to this repository.
 * Documentation is provided in the README.md on how the batch job works, and how 
   to run it. The instructions on how to run it should be very clear, and running 
   the job on the example data set should require as little configuration as possible.
 * Any information, (dummy)-data, files, and other assets that are needed to run 
   this service, are provided in this repository. This includes a docker-compose.yml 
   if required.
   
   
### Tips

Here are some tips to get you going:

 * Think of what you're building as a real reusable piece of software. Think of your 
   end-users and what they want. They need to be able to run it every day, with 
   little hassle. Try and make your code as future proof as possible, thinking 
   about possible other feature requests, before they arise.
 * If you need to store any intermediate data, think hard on what kind of storage
   format best suits this use case.
 * There could be invalid json in the input data, your solution should be able to
   gracefully handle these invalid events.
 * Design a proper data model for reading the input, and any intermediate steps.
 * Design your data flow in an intuitive way, design it according to the best 
   practices of the technology you choose.
 * There are many existing libraries/packages that can solved common problems for you.
   Don't hesitate to use a library where applicable, instead of writing everything 
   from scratch (eg. json parsing, csv parsing, date util libraries)
 * For any decision you make - like technology choices, framework choices, etc. - 
   you should be able to put forth good arguments. You should basically be able to 
   defend your choices.
 * Work in an agile way! You might not be able to implement all of the features 
   for this assignment in the alotted time, so be smart in picking the features you 
   work on first.
 * If you have any questions, feel free to send us an email!

