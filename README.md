# Street Group Pipeline Task

I have completed the pipeline to the point where it generates the JSON file. If I had more time I would continue to develop the pipeline and complete the requirement for the grouping. This is my first time using Apache Beam but I think it went well.

## Design

I decided to complete my pipeline in a procedural fashion as I thought this would be simplest for this task. My code uses functions as a way to split out the code to make it easier to read and manage.

I have included logging and exception handling to catch any problems with the process.

### File Load

In my code, I get the file directly from the website for ease of use. This is a simple HTTP request, in a real case scenario, I would also expect this to be used although possibly an API or other more reliable source.

### File Read

I use the Apache Beam read file function to get the data into the pipeline.

### File CSV Parse

I then parse the CSV using the Python package, the reason for this step is so then the Python is correctly parsed for example, escaping any commas in address fields. The list this creates then allowed to generate the JSON.

### File JSON Generation

This is the section I had most problems with, I was trying to find an Apache Beam process to convert to JSON and add keys but could not find a way to do it so I created another custom function. This loops through the values for each row and adds the key, the row is then converted into JSON from a dictionary.

### Convert Price to Integer

As a small thing to improve accuracy, I wanted to convert the price field into an integer from a string. In a real case scenario, this would then likely be implemented more easily into the system it's being used. This also improves data quality.

### File Output

I used the built-in Beam file writer for this, giving it a .json suffix.

## Improvements and Retrospective

If I was to do this task again, I think I would focus less on the details like logging and exceptions and focus more on the core requirments to get it finished.

If I had longer, I would also like to learn more about Apache Beam and try to improve the way I interact with the framework. Most of my pipeline runs custom made functions rather than any built-in.

I would also like to add testing into the code for example, making sure a valid JSON format is returned.

## Cloud Implementation

In my small experience with Apache Beam so far, it has ran well on my computer using the "Beam Model" so for this specific task, I think in the AWS cloud I would run it on Lambdas or ECS. From research, if a runner approach was required, Google Dataflow would be a good option due to great support and the benefits using cloud services provide e.g. no management of infrastructure, reliable, resilient etc.