# Для исторический данных указывать: кол-во %s, есть ли BETWEEN (если он есть ставим True, иначе False)

bi_sql_queries = {
    1: ["""drop table if exists #Sales%s
           SELECT #SKU.SKU, COUNT(DISTINCT ClientOrder.ID) AS Sales%s
           INTO #Sales%s
           FROM ClientOrder
           JOIN OrderItemEventNew ON OrderItemEventNew.ClientOrderID = ClientOrder.ID
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
    3: """drop table if exists #Size
          select #SKU.SKU, Item.Width, Item.Height, Item.Depth, Item.Weight, Item.VolumeLiter
          into #Size
          from #SKU
          join BeeEye.dbo.Item On Item.ID = #SKU.ItemID
          group by #SKU.SKU, Item.Width, Item.Height, Item.Depth, Item.Weight, Item.VolumeLiter""",
    4: """drop table if exists #Review
          select #SKU.SKU, AVG(CO.Rate) as Rate, COUNT(*) as ReviewsQty
          into #Review
          from #SKU
          left join [DL560SQL].[Wozon].[dbo].[ClientOpinion] as CO with(nolock)
          on CO.ItemID = #SKU.SKU
          group by #SKU.SKU"""
}

bi_table = {0: ["#SKU", "SKU"],
            1: ["#Sales", "Sales"],
            2: ["#Price", "BasePrice", "WebPrice", "StartPrice"],
            3: ["#Size", "Width", "Height", "Depth", "Weight", "VolumeLiter"],
            4: ["#Review", "Rate", "ReviewQty"]}

bi_historical_tables = ['#Sales', '#Price']
