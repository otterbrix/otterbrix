/* Q15. Trusted connection paths through forums created in a given timeframe
\set person1Id 21990232564808
\set person2Id 26388279076936
\set startDate '\'2010-11-01\''::timestamp
\set endDate '\'2010-12-01\''::timestamp
 */
with recursive
srcs(f) as (select :person1Id),
dsts(t) as (select :person2Id),
myForums(id) as (
    select id from Forum f where f.creationDate between :startDate and :endDate
),
mm as (
    select least(msg.CreatorPersonId, reply.CreatorPersonId) as src, greatest(msg.CreatorPersonId, reply.CreatorPersonId) as dst, sum(case when msg.ParentMessageId is null then 10 else 5 end) as w
    from Person_knows_Person pp, Message msg, Message reply
    where true
          and pp.person1id = msg.CreatorPersonId 
          and pp.person2id = reply.CreatorPersonId
          and reply.ParentMessageId = msg.MessageId
          and exists (select * from myForums f where f.id = msg.containerforumid)
          and exists (select * from myForums f where f.id = reply.containerforumid)
    group by src, dst
),
path(src, dst, w) as (
    select pp.person1id, pp.person2id, 10::double precision / (coalesce(w, 0) + 10)
    from Person_knows_Person pp left join mm on least(pp.person1id, pp.person2id) = mm.src and greatest(pp.person1id, pp.person2id) = mm.dst
),
-- bidirectional bfs for nonexistant paths
pexists(src, dir) as (
    (
        select f, true from srcs
        union all
        select t, false from dsts
    )
    union
    (
        with
        ss(src, dir) as (select src, dir from pexists),
        ns(src, dir) as (select p.dst, ss.dir from ss, path p where ss.src = p.src),
        bb(src, dir) as (select src, dir from ns union all select src, dir from ss),
        found as (
            select 1
            from bb b1, bb b2
            where b1.dir and (not b2.dir) and b1.src = b2.src
        )
        select src, dir
        from ns
        where not exists (select 1 from found)
        union all
        select -1, true
        where exists (select 1 from found)
    )
),
pathfound(c) as (
    select 1
    from pexists
    where src = -1 and dir
),
shorts(dir, gsrc, dst, w, dead, iter) as (
    (
        select false, f, f, 0::double precision, false, 0 from srcs where exists (select 1 from pathfound)
        union all
        select true, t, t, 0::double precision, false, 0 from dsts where exists (select 1 from pathfound)
    )
    union all
    (
        with
        ss as (select * from shorts),
        toExplore as (select * from ss where dead = false order by w limit 1000),
        -- assumes graph is undirected
        newPoints(dir, gsrc, dst, w, dead) as (
            select e.dir, e.gsrc as gsrc, p.dst as dst, e.w + p.w as w, false as dead
            from path p join toExplore e on (e.dst = p.src)
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
select coalesce(min(w), -1) from results;
