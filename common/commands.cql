create keyspace demo with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
create table demo.policy(class text, name text, blob text, PRIMARY KEY(class, name));

