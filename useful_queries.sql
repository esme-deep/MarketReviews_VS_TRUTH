UPDATE Staging_Category_Queue SET Status = 'pending', LastAttempt = NULL;


SELECT * FROM Staging_Category_Queue
SELECT * FROM Staging_SubCategory_Queue
SELECT * FROM Staging_Product_Queue
WHERE Status = 'failed'
SELECT * FROM Staging_VDB_Product_Page

select sum(ItemCount) from Staging_SubCategory_Queue

UPDATE Staging_Product_Queue SET Status = 'pending', LastAttempt = NULL WHERE Status = 'failed';

SELECT * FROM dbo.staging_product_cleansed
WHERE Status = 'pending'

SELECT COUNT(Average_rating) FROM dbo.staging_product_cleansed


select count(distinct SKU) from dbo.staging_product_cleansed


