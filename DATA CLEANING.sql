CREATE TABLE layoffs_staging 
LIKE layoffs;

-- 2️ Insert all data from `layoffs` into `layoffs_staging`
INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- 3️ Verify that data is copied successfully
SELECT * FROM layoffs_staging;

WITH duplicate_cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY company, industry, total_laid_off, percentage_laid_off, 
                            `date`, stage, country, funds_raised_millions
               ORDER BY company  -- Ensures consistent row ordering
           ) AS num_row
    FROM layoffs_staging
)
-- 5️ Retrieve duplicate records (excluding the first occurrence)
SELECT *
FROM duplicate_cte
WHERE num_row > 1;

-- 1️⃣ Create a staging table (if not already created)
CREATE TABLE IF NOT EXISTS layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL,
  num_row INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, 
       ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, percentage_laid_off, 
                        `date`, stage, country, funds_raised_millions
           ORDER BY company
       ) AS num_row
FROM layoffs_staging;

-- 3️⃣ Remove duplicate records, keeping only the first occurrence
DELETE FROM layoffs_staging2
WHERE num_row > 1;

select*
from layoffs_staging2;
-- fixing null and errors in company
SELECT *
FROM layoffs_staging2
WHERE company IS NULL OR company = '';

UPDATE layoffs_staging2
SET company = NULL
WHERE company = '';

SELECT DISTINCT company
FROM layoffs_staging2
WHERE company LIKE ' %'  -- Leading space
   OR company LIKE '% '  -- Trailing space
   OR company LIKE '%  %'  -- Multiple spaces
ORDER BY company;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT company
FROM layoffs_staging2
ORDER BY company;

-- location 

SELECT *
FROM layoffs_staging2
WHERE location IS NULL OR location = '';

UPDATE layoffs_staging2
SET location = NULL
WHERE location = '';

SELECT DISTINCT location
FROM layoffs_staging2
WHERE location LIKE ' %' OR location LIKE '% ' OR location LIKE '%  %'
ORDER BY location;

UPDATE layoffs_staging2
SET location = TRIM(location);


-- check errors in industry
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

UPDATE layoffs_staging2
SET industry = 'Finance'
WHERE industry = 'Fin-Tech';

UPDATE layoffs_staging2
SET industry = 'Consumer Goods'
WHERE industry = 'Consumer';

UPDATE layoffs_staging2
SET industry = 'HR & Recruiting'
WHERE industry = 'HR';

UPDATE layoffs_staging2
SET industry = 'Food & Beverage'
WHERE industry = 'Food';

UPDATE layoffs_staging2
SET industry = 'Unknown'
WHERE industry IS NULL OR industry = '';

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;


-- total_laid_of

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL OR total_laid_off = '';

UPDATE layoffs_staging2
SET total_laid_off =null
WHERE total_laid_off IS NULL OR total_laid_off = '';

SELECT DISTINCT total_laid_off
FROM layoffs_staging2
ORDER BY total_laid_off;

-- percent laid off column
-- Replace empty strings with NULL
UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE TRIM(percentage_laid_off) = '';

-- Remove percentage signs and ensure only numeric values remain
UPDATE layoffs_staging2
SET percentage_laid_off = REPLACE(percentage_laid_off, '%', '')
WHERE percentage_laid_off LIKE '%';

-- Convert invalid values (e.g., 'Unknown') to NULL
UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off NOT REGEXP '^[0-9]+(\.[0-9]+)?$';

SELECT DISTINCT percentage_laid_off 
FROM layoffs_staging2
ORDER BY percentage_laid_off;


-- date 
SELECT * 
FROM layoffs_staging2 
WHERE `date` IS NULL OR `date` = '';

UPDATE layoffs_staging2
SET `date` = NULL
WHERE `date` IS NULL OR `date` = '';

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- stage 
SELECT * 
FROM layoffs_staging2 
WHERE stage IS NULL OR stage = '';

UPDATE layoffs_staging2
SET stage = 'Unknown'
WHERE stage IS NULL OR stage = '';

UPDATE layoffs_staging2
SET stage = TRIM(stage);

UPDATE layoffs_staging2
SET stage = 'Seed'
WHERE stage IN ('Seed Funding', 'seed', 'SEED');

UPDATE layoffs_staging2
SET stage = 'Series A'
WHERE stage IN ('Series A Funding', 'A Round', 'series a');

UPDATE layoffs_staging2
SET stage = 'Series B'
WHERE stage IN ('Series B Funding', 'B Round', 'series b');

UPDATE layoffs_staging2
SET stage = 'Series C'
WHERE stage IN ('Series C Funding', 'C Round', 'series c');

UPDATE layoffs_staging2
SET stage = 'IPO'
WHERE stage IN ('Public', 'IPO Filing', 'Pre-IPO', 'Post-IPO');

SELECT DISTINCT stage
FROM layoffs_staging2
ORDER BY stage;

-- country

SELECT * 
FROM layoffs_staging2 
WHERE country IS NULL OR country = '';

UPDATE layoffs_staging2
SET country = 'Unknown'
WHERE country IS NULL OR country = '';

UPDATE layoffs_staging2
SET country = TRIM(country);

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE '%.';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

select *
from layoffs_staging2;

-- fundraised millions

SELECT * 
FROM layoffs_staging2 
WHERE funds_raised_millions IS NULL OR funds_raised_millions = '';

UPDATE layoffs_staging2
SET funds_raised_millions = 0
WHERE funds_raised_millions IS NULL OR funds_raised_millions = '';

   
-- replace blank spaces and unknown if the 2 or more simillar columns 

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry = 'Unknown'  -- Corrected string comparison
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
    AND t1.location = t2.location  -- Ensuring same location
SET t1.industry = t2.industry
WHERE t1.industry = 'Unknown'
AND t2.industry IS NOT NULL
AND t2.industry != 'Unknown';  -- Prevent assigning 'Unknown' again

select*
from layoffs_staging2
;


-- deleting the unknown ttl_l_v and p_L_v
delete
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off = 0.00;


ALTER TABLE layoffs_staging2
DROP COLUMN num_row;



    