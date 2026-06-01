mod common;

use sea_orm::entity::prelude::*;
use sea_orm::{ActiveValue::Set, EntityTrait, QuerySelect};

mod user {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "app", table_name = "t")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i64,
        pub name: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

#[tokio::test]
async fn insert_and_find_all_via_entity() {
    let conn = common::open_test_proxy().await;

    user::ActiveModel {
        id: Set(1),
        name: Set("alice".to_string()),
    }
    .insert(&conn)
    .await
    .expect("insert");

    let all = user::Entity::find().all(&conn).await.expect("find all");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, 1);
    assert_eq!(all[0].name, "alice");
}

#[tokio::test]
async fn find_multiple_via_entity() {
    let conn = common::open_test_proxy().await;

    for (id, name) in [(1i64, "alice"), (2, "bob")] {
        user::ActiveModel {
            id: Set(id),
            name: Set(name.to_string()),
        }
        .insert(&conn)
        .await
        .expect("insert");
    }

    let all = user::Entity::find().all(&conn).await.expect("find all");
    assert_eq!(all.len(), 2);
    let names: Vec<&str> = all.iter().map(|m| m.name.as_str()).collect();
    assert!(names.contains(&"alice"));
    assert!(names.contains(&"bob"));
}

#[tokio::test]
async fn find_by_id_relies_on_limit_inlining() {
    let conn = common::open_test_proxy().await;

    for (id, name) in [(1i64, "alice"), (2, "bob"), (3, "carol")] {
        user::ActiveModel {
            id: Set(id),
            name: Set(name.to_string()),
        }
        .insert(&conn)
        .await
        .expect("insert");
    }

    let found = user::Entity::find_by_id(2)
        .one(&conn)
        .await
        .expect("find_by_id")
        .expect("row present");
    assert_eq!(found.id, 2);
    assert_eq!(found.name, "bob");
}

#[tokio::test]
async fn find_with_explicit_limit_and_offset() {
    let conn = common::open_test_proxy().await;

    for (id, name) in [(1i64, "alice"), (2, "bob"), (3, "carol"), (4, "dave")] {
        user::ActiveModel {
            id: Set(id),
            name: Set(name.to_string()),
        }
        .insert(&conn)
        .await
        .expect("insert");
    }

    let page = user::Entity::find()
        .limit(2)
        .offset(1)
        .all(&conn)
        .await
        .expect("paginated find");
    assert_eq!(page.len(), 2);
}
