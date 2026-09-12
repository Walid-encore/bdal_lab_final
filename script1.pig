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

movies_no_header = FILTER movies BY MovieID IS NOT NULL;

roi_movies = FOREACH movies_no_header GENERATE

MovieID,

Title,

Genre,

BudgetUSD,

Global_BoxOfficeUSD,

((Global_BoxOfficeUSD - BudgetUSD) / BudgetUSD) * 100

    AS ROI:double;

profitable_movies = FILTER roi_movies BY ROI > 0;

STORE profitable_movies

INTO 'positive_roi'

USING PigStorage('|');