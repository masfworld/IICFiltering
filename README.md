# Item-to-item collaborative filtering in Apache Spark

I've developed a simple implementation of the Amazon recommendation system, published in 2003.
This is the specification: http://www.cs.umd.edu/~samir/498/Amazon-Recommendations.pdf

### Version
0.1

### Prerequisites
* Apache Spark 1.5.x

### Configuration
* Edit [src/main/config/mysql.config.bak] inserting your configuration data
* Rename [src/main/config/mysql.config.bak] to [src/main/config/mysql.config]
* Edit [src/main/scala/com/sidesna/iicfiltering/managedata/mysql/MysqlImport.scala] detailing your specific queries to get purchases, products and customers data. It's importan doesn't change aliases.


## License

See the [LICENSE](LICENSE.txt) file for license rights and limitations (MIT).