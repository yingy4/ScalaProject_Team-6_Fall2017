## House Sales Price Prediction ##

`You want to buy/sell a house. You are at the right place !! Uhhh! I can predict the estimated Sales Price!`

>This project is for house sales price prediction which can be used by seller/buyer. Once he gets the estimated price he can decide whether to continue with >the real estate agent or not. If he decides to continue, he will enter the contact details. Real estate agent will use these details to contact the customer.

** Data: **
House Prices Dataset on Kaggle:
*https://www.kaggle.com/c/house-prices-advanced-regression-techniques
*https://www.kaggle.com/harlfoxem/housesalesprediction
The data contains around 80 features. Used ZipCode and States dataset to fetch city and stateinformation for Kaggle data.

###*Pre-Processing:*###
We used Spark SQL for loading and pre-processing the data. We removed some of the columns which are not much important. Reformatted few features, combined few features to generate a new feature and simulated few features which we felt important.We replaced the missing/NA values with mean/most occurred value of the respective feature.After joining with zipcode and states data, we saved the prepared model in MySQL.(Before saving, created the database 'scala_project'.)
The data can be found at [Preprocessing Data]()


####Building Prediction Models:####
We used Spark MLLib to build the Linear Regression and Random Forest Regression models. Thea data was loaded from MySQL database and was split into training and test data in the ratio 80:20.We used VectorAssembler to assemble all the features. Built Linear Regression and Random Forest Regression pipeline. Once the model is built evaluated their performance with the test data. For our data, the RMSE for both the models are almost the same. But Random Forest Regression was having little lower RMSE compared to Lonear Regression model. These models are store for prediction of sales price for the user input data in future. The saved models can be found at [Prediction Models]()

####Web Application:####
We built a web application using Play and Alick framework to predict the house sales prices. Here the user enters the house details he is buying/selling and presses estimate button. The user entered house details will be saved in a table 'testhouses'(execute the createTable.sql before running the application and the file can be found at [Estimated Sales Price]() and used for predicting the sales price using the regression model we had already built and saved.The estimated sales price will be shown to the user and he can decide whether to continue with the real estate agent or not. If he decides to continue, he will be taken to another page where he will be entering his/her contact details. 

The real estate agent now can access the contact details of the user and contact him regarding the house sales.

`Exploratory Analysis-Visualization:`
Perforemed analysis on the data to get the insights like which location is having higer house prices, which are factors are higly correlated with sales price, influence of each feature on sales price and number of houses in a range of sales prices and so on. All visualizations are done using Apache Zeppelina and can be found at [Visualizations]()


>This application really helped us in house purchases/sales. Hope this helps you as well!!
