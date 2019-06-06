# Для исторических признаков указывать: кол-во %s (т.е. сколько раз в запросе встречается день)

# sql-запросы для создания временной таблицы с SKU
bi_sku_initialized_query = {
    0: """drop table if exists #SKU
          SELECT top 100 Item.RezonItemID AS SKU, ItemTypeHierarchyPrx.Type AS CatType, BrandPrx.Name AS BrandName, Item.ID as ItemID
          INTO #SKU
          FROM BeeEye.dbo.Item
          JOIN BeeEye.dbo.BrandPrx
          ON Item.BrandID = BrandPrx.ID
          JOIN BeeEye.dbo.ItemTypeHierarchyPrx
          ON ItemTypeHierarchyPrx.TypeID = Item.ItemGroupID
          WHERE Item.EnabledForSale = 1 AND Item.FreeQty > 0""",
    1: """SELECT sku_elas.SKU, ItemTypeHierarchyPrx.Type AS CatType, BrandPrx.Name AS BrandName, Item.ID AS ItemID
          INTO #SKU
          FROM zzzTemp.dbo.NK_SKU_elasticity sku_elas
          LEFT JOIN BeeEye.dbo.Item ON Item.RezonItemID = sku_elas.SKU
          LEFT JOIN BeeEye.dbo.BrandPrx ON Item.BrandID = BrandPrx.ID
          LEFT JOIN BeeEye.dbo.ItemTypeHierarchyPrx ON ItemTypeHierarchyPrx.TypeID = Item.ItemGroupID"""
}

# sql-запросы для создания временной таблицы с фичами
# 3 параметр отвечает за историческую переменную, т.е. есть ли в ней параметр N
# каждый объект - это [sql-запрос, кол-во дней, историческая ли фича (типо нужен ли ему N),
# используется/не используется = 1/0]
bi_sql_queries = {
    1: ["""drop table if exists #Sales
           SELECT #SKU.SKU, SUM(OrderItemEventNew.Qty) AS Sales
           INTO #Sales
           FROM #SKU
           JOIN BeeEye.dbo.OrderItemEventNew with(index=OrderItemEventNew_Date) ON #SKU.ItemID = OrderItemEventNew.ItemID
           WHERE OrderItemEventNew.ActionTypeID = 179832750000
           AND OrderItemEventNew.Date BETWEEN dateadd(day,datediff(day,%s,GETDATE()),0) AND dateadd(day,datediff(day,%s-1,GETDATE()),0)
           GROUP BY #SKU.SKU
           option (recompile)""",
        2, 0, 1],
    2: ["""drop table if exists #SalesN
           SELECT #SKU.SKU, SUM(OrderItemEventNew.Qty) AS SalesN
           INTO #SalesN
           FROM #SKU
           JOIN BeeEye.dbo.OrderItemEventNew with(index=OrderItemEventNew_Date) ON #SKU.ItemID = OrderItemEventNew.ItemID
           WHERE OrderItemEventNew.ActionTypeID = 179832750000
           AND OrderItemEventNew.Date BETWEEN dateadd(day,datediff(day,%s + 1,GETDATE()),0) AND dateadd(day,datediff(day,%s+1,GETDATE()),0)
           GROUP BY #SKU.SKU
           option (recompile)""",
        2, 1, 1],
    3: ["""drop table if exists #Sales2N
           SELECT #SKU.SKU, SUM(OrderItemEventNew.Qty) AS Sales2N
           INTO #Sales2N
           FROM #SKU
           JOIN BeeEye.dbo.OrderItemEventNew with(index=OrderItemEventNew_Date) ON #SKU.ItemID = OrderItemEventNew.ItemID
           WHERE OrderItemEventNew.ActionTypeID = 179832750000
           AND OrderItemEventNew.Date BETWEEN dateadd(day,datediff(day,%s + 1,GETDATE()),0) AND dateadd(day,datediff(day,%s+1,GETDATE()),0)
           GROUP BY #SKU.SKU
           option (recompile)""",
        2, 2, 1],
    4: ["""drop table if exists #Sales3N
           SELECT #SKU.SKU, SUM(OrderItemEventNew.Qty) AS Sales3N
           INTO #Sales3N
           FROM #SKU
           JOIN BeeEye.dbo.OrderItemEventNew with(index=OrderItemEventNew_Date) ON #SKU.ItemID = OrderItemEventNew.ItemID
           WHERE OrderItemEventNew.ActionTypeID = 179832750000
           AND OrderItemEventNew.Date BETWEEN dateadd(day,datediff(day,%s + 1,GETDATE()),0) AND dateadd(day,datediff(day,%s+1,GETDATE()),0)
           GROUP BY #SKU.SKU
           option (recompile)""",
        2, 3, 1],
    5: ["""drop table if exists #Sales4N
           SELECT #SKU.SKU, SUM(OrderItemEventNew.Qty) AS Sales4N
           INTO #Sales4N
           FROM #SKU
           JOIN BeeEye.dbo.OrderItemEventNew with(index=OrderItemEventNew_Date) ON #SKU.ItemID = OrderItemEventNew.ItemID
           WHERE OrderItemEventNew.ActionTypeID = 179832750000
           AND OrderItemEventNew.Date BETWEEN dateadd(day,datediff(day,%s + 1,GETDATE()),0) AND dateadd(day,datediff(day,%s+1,GETDATE()),0)
           GROUP BY #SKU.SKU
           option (recompile)""",
        2, 4, 1],
    6: ["""DROP TABLE IF EXISTS #Price
           SELECT DISTINCT #SKU.SKU, t.WebPrice as WebPrice, t.BasePrice as BasePrice, t.StartPrice as StartPrice
           into #Price
           FROM #SKU
           cross apply(
               select top 1
               case 
               when ihis.SpecialPrice>0 then ihis.SpecialPrice-ihis.DiscountOnSpecialPrice*ihis.SpecialPrice*
               (case when isnull(ihis.ItemDiscount, 0) > 0 then ihis.ItemDiscount else isnull(ihis.VirtualDiscount, 0) end)/100
               else ihis.Price-ihis.Price*(case when isnull(ihis.ItemDiscount, 0) > 0 then ihis.ItemDiscount else isnull(ihis.VirtualDiscount, 0) end)/100
               end as WebPrice, BasePrice, ihis.Price as StartPrice
               FROM BeeEye.dbo.ItemHistory as ihis
               WHERE #SKU.ItemID = ihis.ItemID
               AND ihis.Moment <= CONVERT(DATE, getdate() - %s)
               Order By Moment Desc) t""",
        1, 0, 1],
    7: ["""drop table if exists #Size
          select #SKU.SKU, Item.Width, Item.Height, Item.Depth, Item.Weight, Item.VolumeLiter
          into #Size
          from #SKU
          join BeeEye.dbo.Item On Item.ID = #SKU.ItemID
          group by #SKU.SKU, Item.Width, Item.Height, Item.Depth, Item.Weight, Item.VolumeLiter""",
        0, 0, 1],
    8: ["""drop table if exists #DayOfWeek
           select #SKU.SKU, DATEPART(dw,GETDATE() - 1) as DayOfWeek
           into #DayOfWeek
           from #SKU""",
        0, 0, 1],
    9: ["""drop table if exists #CompetitorQty
           select #SKU.SKU, COUNT(cp.Price) as CompetitorQty
           into #CompetitorQty
           from BeeEye.dbo.CompetitorPrice cp
           join #SKU
           on #SKU.ItemID = cp.ItemID
           where cp.PriceDate between dateadd(day,datediff(day,%s,GETDATE()),0) AND dateadd(day,datediff(day,%s-1,GETDATE()),0)
           group by #SKU.SKU""",
        2, 0, 1],
    10: ["""drop table if exists #MinCompetitorPrice
           select #SKU.SKU, MIN(cp.Price) as MinCompetitorPrice
           into #MinCompetitorPrice
           from BeeEye.dbo.CompetitorPrice cp
           join #SKU
           on #SKU.ItemID = cp.ItemID
           WHERE cp.PriceDate between dateadd(day,datediff(day,%s,GETDATE()),0) AND dateadd(day,datediff(day,%s-1,GETDATE()),0)
           GROUP BY #SKU.SKU""",
         2, 0, 1],
    11: ["""drop table if exists #AvgCompetitorPrice
           select #SKU.SKU, AVG(cp.Price) as AvgCompetitorPrice
           into #AvgCompetitorPrice
           from BeeEye.dbo.CompetitorPrice cp
           join #SKU
           on #SKU.ItemID = cp.ItemID
           WHERE cp.PriceDate between dateadd(day,datediff(day,%s,GETDATE()),0) AND dateadd(day,datediff(day,%s-1,GETDATE()),0)
           GROUP BY #SKU.SKU""",
         2, 0, 1],
    12: ["""drop table if exists #MaxCompetitorPrice
           select #SKU.SKU, MAX(cp.Price) as MaxCompetitorPrice
           into #MaxCompetitorPrice
           from BeeEye.dbo.CompetitorPrice cp
           join #SKU
           on #SKU.ItemID = cp.ItemID
           WHERE cp.PriceDate between dateadd(day,datediff(day,%s,GETDATE()),0) AND dateadd(day,datediff(day,%s-1,GETDATE()),0)
           GROUP BY #SKU.SKU""",
         2, 0, 1],
    13: ["""select s.SKU, CompetitorOzonQty
           into #CompetitorOzonQty
           from #SKU s
           join Garbage.[dbo].[ItemNavigationCategory] inc2
           on s.SKU = inc2.ItemID
           join (select count(#SKU.SKU) as ConcurentOzonQty, NavigationCategoryID
           from Garbage.[dbo].[ItemNavigationCategory] inc
           join #SKU
           on #SKU.SKU = inc.ItemID
           group by NavigationCategoryID) t
           on inc2.NavigationCategoryID = t.NavigationCategoryID""",
         0, 0, 1],
    14: ["""drop table if exists #AvgDiscountN
            select """]
}

# таблица вида #Название feature, features
# используется для создания конечного запроса
bi_table = {0: ["#SKU", "SKU"],
            1: ["#Sales", "Sales"],
            2: ["#SalesN", "SalesN"],
            3: ["#Sales2N", "Sales2N"],
            4: ["#Sales3N", "Sales3N"],
            5: ["#Sales4N", "Sales4N"],
            6: ["#Price", "BasePrice", "WebPrice", "StartPrice"],
            7: ["#Size", "Width", "Height", "Depth", "Weight", "VolumeLiter"],
            8: ["#DayOfWeek", "DayOfWeek"],
            9: ["#CompetitorQty", "CompetitorQty"],
            10: ["#MinCompetitorPrice", "MinCompetitorPrice"],
            11: ["#AvgCompetitorPrice", "AvgCompetitorPrice"],
            12: ["#MaxCompetitorPrice", "MaxCompetitorPrice"],
            13: ["#CompetitorOzonQty", "CompetitorOzonQty"],
            }
