-- создаем витрину для хранения результатов анализа эффективности промо-акций
create table if not exists detn.puma_promo_effectiveness_analysis (
    analysis_id              serial primary key, 					   -- уникальный идентификатор записи
    promo_id                 int not null,                             -- идентификатор акции
    promo_name               varchar(250),                             -- название акции
    discount_percent         decimal(5,2),                             -- скидка в процентах
    start_date               date,                                     -- начало акции
    end_date                 date,                                     -- конец акции
    promo_duration_days      int,                                      -- длительность акции
    total_sales_quantity     bigint,                                   -- общее количество проданных единиц по акции
    total_revenue            decimal(18,2),                            -- выручка по акции 
    avg_daily_sales          decimal(18,2),                            -- среднее количество продаж в день
    avg_daily_revenue        decimal(18,2),                            -- средняя выручка в день
    unique_customers_count   int,                                      -- уникальных клиентов
    unique_products_count    int,                                      -- уникальных товаров
    unique_stores_count      int,                                      -- уникальных магазинов
    conversion_rate          decimal(5,2),                             -- доля охвата клиентов
    effectiveness_ratio      decimal(5,2),                             -- Коэффициент эффективности (факт. выручка / потенциальная без скидки)
    revenue_per_day          decimal(18,2),                            -- выручка в день
    category_id              int,                                      -- категория (nullable, зависит от среза)
    category_name            varchar(250), 							   -- название категории
    department_id 		     int,   								   -- идентификатора отдела
	department_name 	     varchar(250),							   -- название отдела
    region                   varchar(250),                             -- регион
    loyalty_level            int,                                      -- уровень лояльности
    group_sales_quantity     bigint,                                   -- продажи по группе
    group_revenue            decimal(18,2),                            -- выручка по группе
    group_sales_effect       decimal(5,2),                             -- эффективность группы 
    collect_date             timestamp not null default now(),         -- момент сбора
    etl_version              int not null                              -- версия ETL
);
insert into detn.puma_promo_effectiveness_analysis (              
    promo_id,                
    promo_name,              
    discount_percent,        
    start_date,               
    end_date,               
    promo_duration_days,     
    total_sales_quantity,    
    total_revenue,           
    avg_daily_sales,          
    avg_daily_revenue,        
    unique_customers_count,   
    unique_products_count,    
    unique_stores_count,     
    conversion_rate,          
    effectiveness_ratio,     
    revenue_per_day,          
    category_id,          
    category_name,            
    department_id, 		     
	department_name, 	     
    region,                  
    loyalty_level,           
    group_sales_quantity,    
    group_revenue,          
    group_sales_effect,      
    collect_date,         
    etl_version)
with promo_stats as ( -- формируем метрики по каждой промоакции
	select
		prom.promo_id,
		prom.promo_name,
		prom.discount_percent,
		prom.start_date,
		prom.end_date,
		((prom.end_date)::date - (prom.start_date)::date + 1) as promo_duration_days,
		sum(sal.quantity) as total_sales_quantity, 
		sum(sal.quantity * pro.price) * (1 - prom.discount_percent / 100) as total_revenue,
		sum(sal.quantity)::decimal / nullif(count(distinct sal.date),0) as avg_daily_sales, -- среднее количесвто продаж в день, берем только дни в которые были продажи 
		sum(sal.quantity * pro.price)::decimal * (1 - prom.discount_percent / 100) / nullif(count(distinct sal.date),0) as avg_daily_revenue,
		count(distinct sal.customer_id) as unique_customers_count, 
		count(distinct sal.product_id) as unique_products_count,
		count(distinct sal.store_id) as unique_stores_count,
		count(distinct sal.customer_id)::decimal / nullif((select count(distinct customer_id) from sales.customers),0) * 100 as conversion_rate, 
		(1 - prom.discount_percent / 100) as effectiveness_ratio, -- выручку со скидкой/выручка без скидки , это и есть (1 - процент скидки) 
		sum(sal.quantity * pro.price) * (1 - prom.discount_percent / 100) / ((prom.end_date)::date - (prom.start_date)::date + 1) as revenue_per_day
	from sales.promotions as prom
	left join sales.sales as sal -- все акции, даже по которым нет продаж
		on sal.promo_id = prom.promo_id and prom.is_current = 1 
	left join sales.products as pro
	    on sal.product_id = pro.product_id and pro.is_current = 1
	where prom.is_current = 1 -- считаем только акции которые актуальны в данный момент
	group by 
		prom.promo_id,
		prom.promo_name,
		prom.discount_percent,
		prom.start_date,
		prom.end_date,
		promo_duration_days
),
grouped_category as ( -- смотрим метрки по каждой акции в каждой категории
	select 
	  prom.promo_id,
	  cat.category_id,
	  cat.category_name,
	  cat.department_id,
	  dep.department_name,
	  sum(sal.quantity) as group_sales_quantity,
	  sum(sal.quantity * pro.price * (1 - prom.discount_percent / 100)) as group_revenue
	from 
	  sales.promotions as prom
	  left join sales.sales as sal
	    on prom.promo_id = sal.promo_id
	  left join sales.products as pro
	    on sal.product_id = pro.product_id and pro.is_current = 1
	  left join sales.categories as cat
	    on pro.category_id = cat.category_id and cat.is_current = 1
	  left join sales.departments as dep
	    on cat.department_id = dep.department_id and dep.is_current = 1
	where 
	  prom.is_current = 1
	group by 
	  prom.promo_id,
	  cat.category_id,
	  cat.category_name,
	  cat.department_id,
	  dep.department_name
),
grouped_region as ( -- смотрим метрки по каждой акции в каждом регионе
	select 
	  prom.promo_id,
	  st.region,
	  sum(sal.quantity) as group_sales_quantity,
	  sum(sal.quantity * pro.price * (1 - prom.discount_percent/100)) as group_revenue
	from 
	  sales.promotions as prom
	  left join sales.sales as sal 
	  	on prom.promo_id = sal.promo_id
	  left join sales.products as pro
	  	on sal.product_id = pro.product_id and pro.is_current = 1
	  left join sales.stores as st
	  	on sal.store_id = st.store_id and st.is_current = 1
	where 
	  prom.is_current = 1
	group by 
	  prom.promo_id,
	  st.region
),
grouped_loyalty as ( -- смотрим метрки по каждой акции в каждом уровне лояльности
	select 
	  prom.promo_id,
	  cus.loyalty_level,
	  sum(sal.quantity) as group_sales_quantity,
	  sum(sal.quantity * pro.price * (1 - prom.discount_percent/100)) as group_revenue
	from
	  sales.promotions as prom
	  left join sales.sales as sal 
	  	on prom.promo_id = sal.promo_id
	  left join sales.products as pro 
	  	on sal.product_id = pro.product_id and pro.is_current = 1
	  left join sales.customers as cus 
	  	on sal.customer_id = cus.customer_id and cus.is_current = 1
	where 
	  prom.is_current = 1
	group by 
	  prom.promo_id,
	  cus.loyalty_level
),
fin_group as ( -- обединяем все в итоговоу статистку по групперовкам
    select 
        c.promo_id,
        c.category_id,
        c.category_name,
        c.department_id,
        c.department_name,
        r.region,
        l.loyalty_level,
        coalesce(c.group_sales_quantity,r.group_sales_quantity,l.group_sales_quantity) as group_sales_quantity, 
    from grouped_category c
    inner join grouped_region r using(promo_id)
    inner join grouped_loyalty l using(promo_id)
)
select 
	ps.promo_id,
	ps.promo_name,
	ps.discount_percent,
	ps.start_date::date,
	ps.end_date::date,
	ps.promo_duration_days,
	ps.total_sales_quantity,
	ps.total_revenue,
	ps.avg_daily_sales,
	ps.avg_daily_revenue,
	ps.unique_customers_count,
	ps.unique_products_count,
	ps.unique_stores_count,
	ps.conversion_rate,
	ps.effectiveness_ratio,
	ps.revenue_per_day,
	fg.category_id,
	fg.category_name,
	fg.department_id,
	fg.department_name,
	fg.region,
	fg.loyalty_level,
	fg.group_sales_quantity,
	fg.group_revenue,
	fg.group_sales_quantity::decimal / nullif(ps.total_sales_quantity,0) * 100 as group_sales_ratio,
	now() as calculation_date,
	coalesce((select max(etl_version) from detn.puma_promo_effectiveness_analysis),0) + 1 as etl_version
from promo_stats ps
left join fin_group fg on fg.promo_id = ps.promo_id;
