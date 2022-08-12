# spss-modeler-flow-python-examples

These four examples demonstrate how SPSS Modeler flow Extension Model can be used with plain Python to build models and integrate with Watson Machine Learning.

Please refer to my blog post for more details, https://randyphoa.com/fostering-better-synergies-between-spss-modeler-flow-and-plain-python-on-cloud-pak-for-data-ec842f48b36d


# Usage

- Clone this repo and upload the assets into a Watson Studio project

- Populate the necessary credentials, space and project ID

- Modify as required and run the streams


# Example 1: Build models using Python libraries

A business user with minimal programming knowledge wants to visually apply some complex business rules during the data preparation stage for different business scenarios and train a LightGBM model which is not available out of the box at the time of writing.

![image](https://miro.medium.com/max/1400/1*X8fpvusRp9TVXiq3wB5wgA.png)


# Example 2: Invoke deployed models/functions from Watson Machine Learning

A complex/advance model is developed by the Data Science team and deployed on Watson Machine Learning.

The business users could then visually apply business rules and preprocess the data as required, before finally making a prediction using the deployed model.

![image](https://miro.medium.com/max/1400/1*wcBXJJlM59GsEiPNRyCHsQ.png)


# Example 3: Import Python scripts from Watson Studio/Watson Machine Learning

An alternative to using the code editor in Extension Model node. 

![image](https://miro.medium.com/max/1400/1*ojRMKBGp-LpeTGUT71Oljg.png)


# Example 4: Complete end-to-end example with SQL pushback

End-to-end example leveraging SQL pushback to apply complex business rules and leverage deployed models and function from Watson Machine Learning

![image](https://miro.medium.com/max/1400/1*YM30YF5UAz9l77PN8Gj6Tw.png)
