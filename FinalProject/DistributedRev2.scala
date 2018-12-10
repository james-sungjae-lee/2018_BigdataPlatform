// BigdataPlatform Project
// Rev2 Algorithm with Distributed Computing
// 20132651 Sungjae Lee

//***********************//
//** Get Data From CSV **//
//***********************//

// Review data load from hdfs and parsing csv
val amazon = sc.textFile("hdfs:///amazon/review.csv")
val amazon_temp= amazon.map(x => x.split(','))
val amazon_split = amazon_temp.map(x => (x(0), x(1), x(2).toDouble))

// User / Products data from split data
val users = amazon_split.map(x => x._1).distinct()
val products = amazon_split.map(x => x._2).distinct()

//***********************//
//** Initialize G/F/R  **//
//***********************//

// Initialize User Fairness : fair_0 (userId, fairness)
val fair_0 = users.map(x => (x, 0.75))

// Initialize Review Reliability : rel_0 (userId, productId, reliability)
val rel_0 = amazon_split.map(x => (x._1, x._2, x._3, 0.75))

// Initialize Product Goodness : good_0 (productId, goodness)
val good_0 = products.map(x => (x, 0.75))

//***********************//
//** calculate G/F/R  **//
//***********************//

// 1. calculating 1st Product Goodness : good_1 (productId, good_1)
val good_temp1= rel_0.map(x => (x._2, x._3 * x._4))
val good_temp2 = good_temp1.groupByKey()
val good_temp3 = good_temp2.map(x => (x._1, x._2, x._2.size))
val good_1 = good_temp3.map(x => (x._1, x._2.sum / x._3))

// 2. calculating 1st Review Reliability : rel_1 (userId, productId, rel_1)
// Join Score data and fairness data by userId
// Result is (userId, ((userId, productId, score), fair_0 )) form tuple
val rel_temp1 = amazon_split.map(x => (x._1, (x._1, x._2, x._3)))
val rel_temp2 = rel_temp1.join(fair_0)

// Join rel_temp2 data and goodness data by productId
// Result is (userId, productId, score, fair_0, good_1 ) form tuple
val rel_temp3 = rel_temp2.map(x => (x._2._1._2, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2 )))
val rel_temp4 = rel_temp3.join(good_1)
val rel_temp5 = rel_temp4.map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2))

// calculate rel_1 data and result is (userId, productId, rel_1 ) for tuple
val rel_1 = rel_temp5.map(x => (x._1, x._2, (x._4 + 1 - math.abs(x._3 - x._5)/2)/2))

// 3. calculating 1st User Fairness : fair_1 (userId, fair_1)
// Fairness is mean of user review's reliability.
// Grouping by user, and sum all reliability, and divide by number of reviews
// Result is (userId, fair_1) form tuple
val fair_temp1 = rel_1.map(x => (x._1, x._3))
val fair_temp2 = fair_temp1.groupByKey()
val fair_temp3 = fair_temp2.map(x => (x._1, x._2, x._2.size))
val fair_1 = fair_temp3.map(x => (x._1, x._2.sum / x._3 ))

//***********************//
//** calculate Errors **//
//***********************//

// 1. calculating 1st Fairness Error : fair_error1 (Double)
// Join fair_1 and fair_0 by userid
// calculate Mean Abs Error with map-reduce function
val fair_error_temp = fair_1.join(fair_0)
val fair_error_cal = fair_error_temp.map(x => (x._1, math.abs(x._2._1 - x._2._2)))
val fair_error1 = fair_error_cal.map(x => x._2).reduce( _ + _ )

// 2. calculating 1st Goodness Error : good_error1 (Double)
// Join good_1 and good_0 by productid
// calculate Mean Abs Error with map-reduce function
val good_error_temp = good_1.join(good_0)
val good_error_cal = good_error_temp.map(x => (x._1, math.abs(x._2._1 - x._2._2 )))
val good_error1 = good_error_cal.map(x => x._2).reduce( _ + _ )

// 3. calculating 1st Reliability Error : rel_error1 (Double)
// Join rel_1 and rel_0 by new review id, (userid + "_" + productid) form string
// calculate Mean Abs Error with map-reduce function
val rel_1_temp = rel_1.map(x => (x._1 + "_" + x._2, x._3))
val rel_0_temp = rel_0.map(x => (x._1 + "_" + x._2, x._3))
val rel_error_temp = rel_1_temp.join(rel_0_temp)
val rel_error_cal = rel_error_temp.map(x => (x._1, math.abs(x._2._1 - x._2._2)))
val rel_error1 = rel_error_cal.map(x => x._2).reduce( _ + _ )

// 4. Find maximum error with reduce function
val error_list1 = List(fair_error1, good_error1, rel_error1)
val max_error1 = error_list1.reduce( (x, y) => math.max(x, y) )

//***********************//
//** 1st Cycle Result  **//
//***********************//

// 1. Find fraud users
fair_1.takeOrdered(10)(Ordering[Double].on(x => x._2)).foreach(println)

// 2. Find bad quality products
good_1.takeOrdered(10)(Ordering[Double].on(x => x._2)).foreach(println)

// 3. Find not fair reviews
rel_1.takeOrdered(10)(Ordering[Double].on(x => x._3)).foreach(println)
