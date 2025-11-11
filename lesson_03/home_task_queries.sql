/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
select c.category_id, c.name, count(1) as films_count
from film_category fc
inner join category c on c.category_id  = fc.category_id 
group by c.category_id, c.name
order by films_count desc;



/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
select a.actor_id, a.first_name || ' ' || a.last_name as actor_name, count(1) as rental_count
from rental r
inner join inventory i on i.inventory_id  = r.inventory_id 
inner join film_actor fa on fa.film_id  = i.film_id 
inner join actor a on a.actor_id  = fa.actor_id 
group by a.actor_id, a.first_name || ' ' || a.last_name
order by rental_count desc
limit 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
select c.category_id, c.name, sum(pi.amount) as total_amt
from rental r 
inner join payment pi on pi.rental_id  = r.rental_id 
inner join inventory i on i.inventory_id  = r.inventory_id 
inner join film_category fc on fc.film_id  = i.film_id 
inner join category c on c.category_id  = fc.category_id
group by c.category_id, c.name
order by total_amt desc
limit 1;



/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
select f.title
from film f
left join inventory i on i.film_id  = f.film_id 
where i.inventory_id is null;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
with children_category_base as
(select f.film_id
from film f
inner join film_category fc  on fc.film_id  = f.film_id 
inner join category c on c.category_id  = fc.category_id 
where c.name = 'Children')
select a.actor_id, a.first_name || ' ' || a.last_name as actor_name, count(cb.film_id) as films_count
from children_category_base cb
inner join film_actor fa on fa.film_id  = cb.film_id 
inner join actor a on a.actor_id  = fa.actor_id 
group by a.actor_id, a.first_name || ' ' || a.last_name
order by films_count desc
limit 3;
