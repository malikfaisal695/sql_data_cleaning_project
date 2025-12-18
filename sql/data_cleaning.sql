-- Data cleaning 

Select *
 from layoffs;
 
 -- 1. Remove Duplicates.
 -- 2. Standarize the data.
 -- 3. Null values and Blank values.
 -- 4. Remove any column or rows.
 
 -- creating a sample table to work with.
 
 Create table layoffs_staging
 like layoffs;
 
 select * from layoffs_staging ;
 
 insert into layoffs_staging 
 select *
 from layoffs;
 
 select * from layoffs_staging;

-- Removing the duplicates.
Select * ,
Row_Number () over ( 
Partition by company, industry, total_laid_off, percentage_laid_off, `date` ) as row_num 
from layoffs_staging;
 
 with duplicate_cte as
 (Select * ,
Row_Number () over ( 
Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) as row_num 
from layoffs_staging
)
select * 
from duplicate_cte
Where row_num > 1;

-- Creating another sample table on which we will delete the rows 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2;

Insert into layoffs_staging2 
Select * ,
Row_Number () over ( 
Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) as row_num 
from layoffs_staging;

select * 
from layoffs_staging2
where row_num > 1;

Delete
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2
where row_num > 1;



-- Standardizing data 
select Company, trim(company)
from layoffs_staging2;

Update layoffs_staging2
set company = trim(company);

-- Removing different name to a same entity 
select  distinct industry 
from layoffs_staging2
order by industry;

select *
from layoffs_staging2
where industry like 'crypto%';

Update layoffs_staging2
set industry = 'Crypto'
Where industry Like 'crypto%';

select *
from layoffs_staging2
where industry like 'crypto%';
-- Done--------------------

Select distinct country, trim(trailing '.' from country) as g_country
from layoffs_staging2
order by 1;

update layoffs_staging2
set  country = trim(trailing '.' from country)
where country like 'United States%' ;

-- Date formating 
Alter table layoffs_staging2
modify column `date` date;

Select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select * 
from layoffs_staging2;
-- done ---------

-- Looking for null values in the industry column 

select * 
from layoffs_staging2
Where industry is Null 
or industry = '';

select *
from layoffs_staging2
where company like '%Airbnb%'  ;

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
  on t1.company = t2.company
  and t1.location = t2.location
where t1.industry is Null 
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
  on t1.company = t2.company
  and t1.location = t2.location
  set t1.industry =t2.industry
  where t1.industry is Null 
and t2.industry is not null; 

select *
from layoffs_staging2
where total_laid_off is Null
and percentage_laid_off is null;

Delete
from layoffs_staging2
where total_laid_off is Null
and percentage_laid_off is null;

select *
from layoffs_staging2;


alter table layoffs_staging2
drop column row_num;

-- Hence the data is somehow ready for analises 
