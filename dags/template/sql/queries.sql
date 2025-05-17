-- How many countries speaks French
SELECT COUNT(*) AS "Number of French Speaking countries"
FROM worldcountries
WHERE language = 'French';

-- How many countries speaks english
SELECT COUNT(*) AS "Number of English Speaking countries"
FROM worldcountries
WHERE language = 'English';

-- How many country have more than 1 official language
SELECT COUNT(*) AS "Countries with more than one language"
FROM worldcountries
WHERE no_languages > 1;

-- How many country official currency is Euro
SELECT COUNT(*) AS "Countries spending Euro"
FROM worldcountries
WHERE currency_name = 'Euro';

-- How many country is from West europe
SELECT COUNT(*) AS "Countries from West Europe"
FROM worldcountries
WHERE subregion = 'Western Europe';

-- How many country has not yet gain independence
SELECT COUNT(*) AS "Countries not independent"
FROM worldcountries
WHERE independence = false;

-- How many distinct continent and how many country from each
SELECT DISTINCT continent, COUNT(*) as "Number of countries"
FROM worldcountries
GROUP BY continent;

-- How many country whose start of the week is not Monday
SELECT COUNT(*) as "Countries not starting week on monday"
FROM worldcountries
WHERE startofweek != 'monday';

-- How many countries are not a United Nation member
SELECT COUNT(*) as "Countries not UN member"
FROM worldcountries
WHERE un_member = false;

-- How many countries are United Nation member
SELECT COUNT(*) as "UN member countries"
FROM worldcountries
WHERE un_member = true;

-- Least 2 countries with the lowest population for each continents
WITH two_least_country AS 
(
SELECT continent, country_name, population,
ROW_NUMBER() OVER(PARTITION BY continent ORDER BY population) as rank_no
FROM worldcountries
)
SELECT continent, country_name, population
FROM two_least_country
WHERE rank_no <= 2;

-- Top 2 countries with the largest Area for each continent
WITH top_two_countries AS 
(
SELECT continent, country_name, area,
ROW_NUMBER() OVER(PARTITION BY continent ORDER BY area DESC) as rank_no
FROM worldcountries
)

SELECT continent, country_name, area
FROM top_two_countries
WHERE rank_no <= 2;

-- Top 5 countries with the largest Area
SELECT country_name, area
FROM worldcountries
ORDER BY area DESC
LIMIT 5;

-- Top 5 countries with the lowest Area
SELECT country_name, area
FROM worldcountries
ORDER BY area
LIMIT 5;

-- Total Population by Continent
SELECT continent, SUM(population) as "Total Population"
FROM worldcountries
GROUP BY continent
ORDER BY "Total Population" DESC;

-- Top 5 speaking lanugages by number of countries
SELECT language, count(*) as "Number of speaking countries"
FROM worldcountries
GROUP BY language
ORDER BY "Number of speaking countries" DESC;

-- Number of countries in the world 
SELECT count(*) as "Number of distinct countries"
FROM (
SELECT DISTINCT country_name
FROM worldcountries
) as countries;