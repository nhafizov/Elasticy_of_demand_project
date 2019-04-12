# Для исторических признаков указывать: кол-во %s (т.е. сколько раз в запросе встречается день)

bi_sku_initialized_query = {
    0: """drop table if exists #SKU
          SELECT top 500 Item.RezonItemID AS SKU, ItemTypeHierarchyPrx.Type AS CatType, BrandPrx.Name AS BrandName, Item.ID as ItemID
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

bi_sql_queries = {
    1: ["""drop table if exists #Sales%s
           SELECT #SKU.SKU, SUM(OrderItemEventNew.Qty) AS Sales%s
           INTO #Sales%s
           FROM BeeEye.dbo.OrderItemEventNew
           JOIN #SKU ON #SKU.ItemID = OrderItemEventNew.ItemID
           WHERE OrderItemEventNew.ActionTypeID = 179832750000
           AND OrderItemEventNew.Date BETWEEN dateadd(day,datediff(day,%s,GETDATE()),0) AND dateadd(day,datediff(day,%s-1,GETDATE()),0)
           GROUP BY #SKU.SKU
           option (recompile)""",
        5],
    2: ["""DROP TABLE IF EXISTS #Price%s
           SELECT DISTINCT #SKU.SKU, t.WebPrice as WebPrice%s, t.BasePrice as BasePrice%s, t.StartPrice as StartPrice%s
           into #Price%s
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
               AND ihis.Moment < CONVERT(DATE, getdate() - %s - 1)
               Order By Moment Desc) t""",
        6],
    3: ["""drop table if exists #Size
          select #SKU.SKU, Item.Width, Item.Height, Item.Depth, Item.Weight, Item.VolumeLiter
          into #Size
          from #SKU
          join BeeEye.dbo.Item On Item.ID = #SKU.ItemID
          group by #SKU.SKU, Item.Width, Item.Height, Item.Depth, Item.Weight, Item.VolumeLiter""",
        0]
}

# таблица вида #Название feature, features
bi_table = {0: ["#SKU", "SKU"],
            1: ["#Sales", "Sales"],
            2: ["#Price", "BasePrice", "WebPrice", "StartPrice"],
            3: ["#Size", "Width", "Height", "Depth", "Weight", "VolumeLiter"]
            }

# исторические признаки
bi_historical_tables = ['#Sales', '#Price']
