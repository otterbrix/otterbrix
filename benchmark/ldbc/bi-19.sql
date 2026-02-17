/* Q19. Interaction path between cities
\set city1Id 608
\set city2Id 1148
 */
with recursive
srcs(f) as (select id from Person where locationcityid = :city1Id),
dsts(t) as (select id from Person where locationcityid = :city2Id),
shorts(dir, gsrc, dst, w, dead, iter) as (
    (
        select false, f, f, 0::double precision, false, 0 from srcs
        union all
        select true, t, t, 0::double precision, false, 0 from dsts
    )
    union all
    (
        with
        ss as (select * from shorts),
        toExplore as (select * from ss where dead = false order by w limit 1000),
        -- assumes graph is undirected
        newPoints(dir, gsrc, dst, w, dead) as (
            select e.dir, e.gsrc as gsrc, p.dst as dst, e.w + p.w as w, false as dead
            from PathQ19 p join toExplore e on (e.dst = p.src)
            union all
            select dir, gsrc, dst, w, dead or exists (select * from toExplore e where e.dir = o.dir and e.gsrc = o.gsrc and e.dst = o.dst) from ss o
        ),
        fullTable as (
            select distinct on(dir, gsrc, dst) dir, gsrc, dst, w, dead
            from newPoints
            order by dir, gsrc, dst, w, dead desc
        ),
        found as (
            select min(l.w + r.w) as w
            from fullTable l, fullTable r
            where l.dir = false and r.dir = true and l.dst = r.dst
        )
        select dir,
               gsrc,
               dst,
               w,
               dead or (coalesce(t.w > (select f.w/2 from found f), false)),
               e.iter + 1 as iter
        from fullTable t, (select iter from toExplore limit 1) e
    )
),
ss(dir, gsrc, dst, w, iter) as (
    select dir, gsrc, dst, w, iter from shorts where iter = (select max(iter) from shorts)
),
results(f, t, w) as (
    select l.gsrc, r.gsrc, min(l.w + r.w)
    from ss l, ss r
    where l.dir = false and r.dir = true and l.dst = r.dst
    group by l.gsrc, r.gsrc
)
select * from results where w = (select min(w) from results) order by f, t;
