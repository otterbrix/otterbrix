/* Q20. Recruitment
\set person2Id 32985348834889
\set company 'Express_Air'
 */
with recursive
srcs(f) as (select :person2Id),
dsts(t) as (
    select personid
    from Person_workat_company pwc, Company c
    where pwc.companyid = c.id and c.name=:company
),
-- Try to find any path with a faster two way BFS
anyPath(pos) as (
    select f from srcs
    union
    (
        with
        ss as (select pos from anyPath)
        select dst
        from ss, PathQ20
        where pos = src and not exists (select 1 from ss, dsts where ss.pos = dsts.t)
    )
),
pathExists as (
    select 1 where exists (select 1 from anyPath ss, dsts where ss.pos = dsts.t)
),
shorts(dir, gsrc, dst, w, dead, iter) as (
    (
        select false, f, f, 0, false, 0 from srcs where exists (select 1 from pathExists)
        union all
        select true, t, t, 0, false, 0 from dsts where exists (select 1 from pathExists)
    )
    union all
    (
        with
        ss as (select * from shorts),
        toExplore as (select * from ss where dead = false order by w limit 1000),
        -- assumes graph is undirected
        newPoints(dir, gsrc, dst, w, dead) as (
            select e.dir, e.gsrc as gsrc, p.dst as dst, e.w + p.w as w, false as dead
            from PathQ20 p join toExplore e on (e.dst = p.src)
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
select t, w from results where w = (select min(w) from results) order by t limit 20;
