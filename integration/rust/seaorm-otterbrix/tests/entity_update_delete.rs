mod common;

use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;

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
async fn entity_update_changes_field() {
    let conn = common::open_test_proxy().await;

    user::ActiveModel {
        id: Set(1),
        name: Set("alice".into()),
    }
    .insert(&conn)
    .await
    .expect("insert seed");

    let model = user::Entity::find_by_id(1)
        .one(&conn)
        .await
        .expect("find_by_id")
        .expect("row present");
    let mut active: user::ActiveModel = model.into();
    active.name = Set("alice2".into());
    active.update(&conn).await.expect("update");

    let updated = user::Entity::find_by_id(1)
        .one(&conn)
        .await
        .expect("find_by_id after update")
        .expect("row present after update");
    assert_eq!(updated.id, 1);
    assert_eq!(updated.name, "alice2");
}

#[tokio::test]
async fn entity_delete_by_id_removes_row() {
    let conn = common::open_test_proxy().await;

    for (id, name) in [(1i64, "a"), (2, "b")] {
        user::ActiveModel {
            id: Set(id),
            name: Set(name.into()),
        }
        .insert(&conn)
        .await
        .expect("insert seed");
    }

    user::Entity::delete_by_id(1)
        .exec(&conn)
        .await
        .expect("delete_by_id");

    let remaining = user::Entity::find()
        .all(&conn)
        .await
        .expect("find all after delete");
    assert_eq!(remaining.len(), 1, "only one row should remain");
    assert_eq!(remaining[0].id, 2);
    assert_eq!(remaining[0].name, "b");
}
