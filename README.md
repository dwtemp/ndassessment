I've run out of time to make a fancy README (or implement proper testing), but here's some collected thoughts:

  1. I've presented a handful of different ways to present the data. This would ideally be clarified with the end user instead.
  2. I've made use of SQL Queries, as this is my preferred way of working. I appreciate their shortcomings, especially considering debugging is easier with Spark methods.
  3. Reusable components are stored in classes / functions. More could have been done (eg, compressing `read_query` and running it to generate a new DataFrame) to abstract further, but I'm only realising that writing this now.
  4. Whilst all datasets use the `::` seperator, I decided to include this as a parameter for the `read_data` method anyway.
  5. Running the script will always generate the `dataOut` folder, arguably earlier than is needed given failures can happen later on.
  6. This should be operating system agnostic. However, I did this at home on my Windows machine, and I would have much rather used a Mac or Linux.
  7. The `Movie` dataset's `Genre` column would have been fun to explode, but there was no use case for it.
  8. Year could easily be `split` out from the `Title` of each movie, but again, no use case.

As mentioned, I've not had time to implement testing, so here's some thoughts on that:

Regarding code testing:
  1. I do not believe there is much room for unit testing the Python code here. The `FileHelpers` class is basic and does not perform anything complex or behave as part of a large ecosystem. A unit test covering this might become necessary in a larger application.
  2. Integration testing is more suited here; ie, providing a set of input data with planned 'gotcha's and asserting whether the generated output matches a target output dataset.
  3. Not quite testing, but there's no try-except or error handling, which there should be in a few places to make the code nicer to work with (eg, on file read and write, in particular).

Regarding data testing (largely DQ):
  1. Had I not run out of free time, I would have liked to dive into this more.
  2. As the data README pointed out, movie `Title`s are likely poor and unreliable. Actually fixing this data would have been complicated. As we had `MovieID` though, there was little need to fix `Title`, as all data operations would happen against `MovieID`. That said, picking the better of conflicting titles would be necessary if forming this into a dimension table as part of a star schema or similar.
  3. I didn't validate the `Timestamp` column. As this is a generated value, the odds of error are fairly low, though.
  4. I did a small amount of manual testing to find that there are a minimum of 20 ratings per user, which does mean there's no scenario where we're handling just 1 or 2 `Preferences` for the Top 3 table.
  5. There should be a test to ensure `Rating`s are only 1, 2, 3, 4, or 5.
  6. There should be no blank entry in any cell for any read or produced dataset.
  7. `MovieID` should always be unique and never blank (Primary Key rules).
  8. Particularly as part of the Top3 generation, there should be tests to ensure the final result is equal to or less than the unique count of `UserID` * 3.

Cheers, DW
