1. How have revenue, cost, and profit margins trended year-over-year?
SELECT [Year],
	   Total_Revenue,
	   Operational_Cost,
	   (Total_Revenue-Operational_Cost) AS Profit,
	  FORMAT(ROUND(CAST((Total_Revenue-Operational_Cost) AS float)/Total_Revenue,3),'P') AS Profit_Margin
FROM(
SELECT  DATEPART(YEAR,transaction_date) AS [Year],
		SUM(transaction_amount_ngn) AS Total_Revenue,
	    SUM(cost_to_serve_ngn) AS Operational_Cost
FROM Silver.crm_transaction
GROUP BY DATEPART(YEAR,transaction_date)
) T
ORDER BY [Year] ASC;
GO

