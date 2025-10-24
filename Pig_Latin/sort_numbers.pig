-- Load the data
numbers = LOAD 'input.txt' AS (num:int);

-- Sort the data in ascending order
sorted_numbers = ORDER numbers BY num ASC;

-- Store the sorted data into output folder
STORE sorted_numbers INTO 'output_numbers';
