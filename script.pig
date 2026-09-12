
movies = LOAD 'movies_dataset.csv'
USING PigStorage(',')
AS (
    MovieID:int,
    Title:chararray,
    Genre:chararray,
    ReleaseYear:int,
    ReleaseDate:chararray,
    Country:chararray,
    BudgetUSD:double,
    US_BoxOfficeUSD:double,
    Global_BoxOfficeUSD:double,
    Opening_Day_SalesUSD:double,
    One_Week_SalesUSD:double,
    IMDbRating:double,
    RottenTomatoesScore:double,
    NumVotesIMDb:int,
    NumVotesRT:int,
    Director:chararray,
    LeadActor:chararray
);

-- ORDER: highest global revenue first
ordered = ORDER movies BY Global_BoxOfficeUSD DESC;

-- LIMIT: keep only the top 10
top10 = LIMIT ordered 10;

-- Select only Title and Global Revenue
-- Convert revenue from double to long to avoid scientific notation
result = FOREACH top10 GENERATE
    Title,
    (long)Global_BoxOfficeUSD AS GlobalRevenue;

-- Save Top 10 output
STORE result INTO 'top10_global_revenue'
USING PigStorage('|');