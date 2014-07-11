A Decision Tree algorithm for Apache SPARK
=============

This repository focus on a possible implementation of a decision tree for Apache SPARK.

This repository is contains packages that helps building two kinds of tree models: Classification Tree and Regression Tree on the scalable enviroment with platform SPARK.

Paricularly, Regression Tree is implemented from CART algorithm of Breiman, which was introduced in 1984.
Classification Tree has two types: Binary Classification Tree (implemented by CART ) and Multi-way Classification Tree (built by ID3).

Besides, you can do cross validation to prune tree models or use Random Forest to aggregate and increase the accuracy of prediction.

You can find some information about our work here:

1. [Overview about decision tree](https://github.com/bigfootproject/spark-dectree/wiki/Overview-of-Regression-Tree)

2. [Regression Tree with CART algorithm](https://github.com/bigfootproject/spark-dectree/wiki/Regression-Tree-with-CART-algorithms)

3. [How to use Regression Tree in this repository](https://github.com/bigfootproject/spark-dectree/wiki/How-to-use-our-library)

