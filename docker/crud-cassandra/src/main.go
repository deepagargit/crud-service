
package main

import
(
   "fmt"
   "log"
   "github.com/gin-gonic/gin"
//   "strconv"
   "github.com/gocql/gocql"
//   "github.com/hashicorp/consul/api"
)


type Policy struct {
    Class string `db:"class" json:"class"`
    Name string `db:"name" json:"name"`
    Blob string `db:"blob" json:"blob"`
}

/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace demo with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
create table demo.policy(class text, name text, blob text, PRIMARY KEY(class, name));

// Being primary key, below won;t work
create index on demo.policy(class);
create index on demo.policy(name);

curl -X PUT http://localhost:8500/v1/agent/service/register -d '{"ID":"cassandra", "name":"cassandra","tags": ["rails"], "port": 9160, "Address":"192.168.0.168"}'
curl http://localhost:8500/v1/catalog/service/cassandra | python -m json.tool
*/


func checkErr(err error, msg string) {
    if err != nil {
        log.Fatalln(msg, err)
    }
}


func main() {
  r := gin.Default()
  v1 := r.Group("api/v1")

  {
  v1.GET("/policies/:class", GetPolicies)
  v1.GET("/policies/:class/:name", GetPolicy)
  v1.POST("/policies", PostPolicy)
  v1.PUT("/policies/:class/:name", UpdatePolicy)
  v1.DELETE("/policies/:class/:name", DeletePolicy)
  }

  r.Run(":8080")
}


func GetCassandraIP() (IP string) {

   //var IP string
/*
   //Get a new client
   client, err := api.NewClient(api.DefaultConfig())
   if err != nil {
      panic(err)
   }

    agent := client.Agent()
    services, err := agent.Services()

    fmt.Println("services : ", services)
        if err != nil {
                log.Fatal("err: %v", err)
        }

        if _, ok := services["cassandra"]; !ok {
                log.Fatal("missing service: %v", services)
        }

    IP = services["cassandra"].Address
    fmt.Println(" Service cassandra IP : ", services["cassandra"].Address)
*/
    IP = "127.0.0.1"
    fmt.Println(" Service cassandra IP : ", IP)
    return

}

// CRUD Operations




// READ all policies

func GetPolicies(c *gin.Context) {

    // connect to the cluster
    cluster := gocql.NewCluster(GetCassandraIP())

    // A keyspace in Cassandra is a namespace that defines data replication on nodes. A cluster contains one keyspace per node.
    cluster.Keyspace = "demo"

    // May use gocql.Quorum
    cluster.Consistency = gocql.LocalOne
    session, _ := cluster.CreateSession()

    // Make sure that the connection can close once you are done.
    defer session.Close()

//    ####################################### Query Logic ##########################################

    class := c.Params.ByName("class")

    fmt.Println("class : ", class)

    var policies []Policy
    var policy Policy

    iter := session.Query("SELECT class, name, blob FROM policy WHERE class=?", class).Iter()
    for iter.Scan(&policy.Class, &policy.Name, &policy.Blob) {

        // fmt.Println("1 : ", policy.Class, policy.Name, policy.Blob)
        policies = append(policies, policy)
    }

    if len(policies) > 0 {
        c.JSON(200, policies)
    } else {
        c.JSON(404, gin.H{"error": "no policy(s) into the table"})
    }

    if err := iter.Close(); err != nil {
        log.Fatal(err)
    }


    // curl -i http://localhost:8080/api/v1/policies/common
}



// Read a user


func GetPolicy(c *gin.Context) {

    // connect to the cluster
    cluster := gocql.NewCluster(GetCassandraIP())

    // A keyspace in Cassandra is a namespace that defines data replication on nodes. A cluster contains one keyspace per node.
    cluster.Keyspace = "demo"

    // May use gocql.Quorum
    cluster.Consistency = gocql.LocalOne
    session, _ := cluster.CreateSession()

    // Make sure that the connection can close once you are done.
    defer session.Close()

//    ####################################### Query Logic ##########################################

    class := c.Params.ByName("class")
    name := c.Params.ByName("name")
    var policy Policy

    err := session.Query("SELECT class, name, blob FROM policy WHERE class=? AND name=?", class, name).Scan(&policy.Class, &policy.Name, &policy.Blob);


    if err == nil {
        c.JSON(200, policy)
    } else {
        c.JSON(404, gin.H{"error": "no policy(s) found"})
    }

    // curl -i http://localhost:8080/api/v1/policies/common/email
}




// Create a user

func PostPolicy(c *gin.Context) {

    // connect to the cluster
    cluster := gocql.NewCluster(GetCassandraIP())

    // A keyspace in Cassandra is a namespace that defines data replication on nodes. A cluster contains one keyspace per node.
    cluster.Keyspace = "demo"

    // May use gocql.Quorum
    cluster.Consistency = gocql.LocalOne
    session, _ := cluster.CreateSession()

    // Make sure that the connection can close once you are done.
    defer session.Close()

//    ####################################### Query Logic ##########################################

    var policy Policy
    if c.BindJSON(&policy) == nil {
       fmt.Println("Bind success", c.Params.ByName("Class"))
    } else {
       fmt.Println("Bind failure")
    }


    fmt.Println("Policy: ", policy.Class, policy.Name, policy.Blob)

    if policy.Class != "" && policy.Name != "" && policy.Blob != "" {

    err := session.Query("INSERT INTO policy (class, name, blob) VALUES (?, ?, ?)", policy.Class, policy.Name, policy.Blob).Exec()


            if err == nil {
              c.JSON(201, policy)

/* // Commenting consul KV store
              // Get a new client
                client, err := api.NewClient(api.DefaultConfig())
                if err != nil {
                    panic(err)
                }

                // Get a handle to the KV API
                kv := client.KV()

                key := "notify" + "/" + "policy" + "/" + policy.Class + "/" + policy.Name
                value := []byte("POST")

                fmt.Println(key, " : ", value)



                // PUT a new KV pair
                p := &api.KVPair{Key: key, Value: value}
                _, err = kv.Put(p, nil)
                if err != nil {
                    panic(err)
                } else {
                        fmt.Println("kv updated")
                }
*/

            } else {
                checkErr(err, "Insert failed")
            }
    } else {
        c.JSON(422, gin.H{"error": "fields are empty"})
    }


    // curl -i -X POST -H "Content-Type: application/json" -d "{ \"class\": \"common\", \"name\": \"email\", \"blob\": \"{rule-2}\" }" http://localhost:8080/api/v1/policies
}


// Update a user

func UpdatePolicy(c *gin.Context) {


    // connect to the cluster
    cluster := gocql.NewCluster(GetCassandraIP())

    // A keyspace in Cassandra is a namespace that defines data replication on nodes. A cluster contains one keyspace per node.
    cluster.Keyspace = "demo"

    // May use gocql.Quorum
    cluster.Consistency = gocql.LocalOne
    session, _ := cluster.CreateSession()

    // Make sure that the connection can close once you are done.
    defer session.Close()

//    ####################################### Query Logic ##########################################

    fmt.Println("In Update")


    var json Policy
    c.BindJSON(&json)

    json.Class = c.Params.ByName("class")
    json.Name = c.Params.ByName("name")


    fmt.Println("json : ", json.Class, json.Name, json.Blob)
    var policy Policy

    err := session.Query("SELECT class, name, blob FROM policy WHERE class=? AND name=?", json.Class, json.Name).Scan(&policy.Class, &policy.Name, &policy.Blob);


    if err == nil {
        fmt.Println("select query success ")
        if json.Blob != "" {

        err := session.Query("UPDATE policy SET blob=? WHERE class=? AND name=?", json.Blob, json.Class, json.Name).Exec();

            if err == nil {
              c.JSON(200, json)

/* // Commenting consul KV

             // Get a new client
                client, err := api.NewClient(api.DefaultConfig())
                if err != nil {
                    panic(err)
                }

                // Get a handle to the KV API
                kv := client.KV()

                key := "notify" + "/" + "policy" + "/" + json.Class + "/" + json.Name
                value := []byte("PUT")

                fmt.Println(key, " : ", value)

                // PUT a new KV pair
                p := &api.KVPair{Key: key, Value: value}
                _, err = kv.Put(p, nil)
                if err != nil {
                    panic(err)
                } else {
                        fmt.Println("kv updated")
                }
*/

            } else {
                checkErr(err, "Update failed")
            }
        } else {
            c.JSON(422, gin.H{"error": "fields are empty"})
        }

    } else {
        fmt.Println("select query failed")
        c.JSON(404, gin.H{"error": "policy not found"})
    }



    // curl -i -X PUT -H "Content-Type: application/json" -d "{ \"blob\": \"{rule-2}\" }" http://localhost:8080/api/v1/policies/common/email
}


// Delete a user

func DeletePolicy(c *gin.Context) {


    // connect to the cluster
    cluster := gocql.NewCluster(GetCassandraIP())

    // A keyspace in Cassandra is a namespace that defines data replication on nodes. A cluster contains one keyspace per node.
    cluster.Keyspace = "demo"

    // May use gocql.Quorum
    cluster.Consistency = gocql.LocalOne
    session, _ := cluster.CreateSession()

    // Make sure that the connection can close once you are done.
    defer session.Close()

//    ####################################### Query Logic ##########################################


    class := c.Params.ByName("class")
    name := c.Params.ByName("name")
    var policy Policy

    err := session.Query("SELECT class, name, blob FROM policy WHERE class=? AND name=?", class, name).Scan(&policy.Class, &policy.Name, &policy.Blob);

    if err == nil {
        if err := session.Query("DELETE FROM policy WHERE class=? AND name=?", class, name).Exec(); err != nil {
                        log.Fatal(err)
            }

        if err == nil {
          c.JSON(200, policy)

/* // Commented consul KV

             // Get a new client
                client, err := api.NewClient(api.DefaultConfig())
                if err != nil {
                    panic(err)
                }

                // Get a handle to the KV API
                kv := client.KV()

                key := "notify" + "/" + "policy" + "/" + policy.Class + "/" + policy.Name
                value := []byte("DELETE")

                fmt.Println(key, " : ", value)

                // PUT a new KV pair
                p := &api.KVPair{Key: key, Value: value}
                _, err = kv.Put(p, nil)
                if err != nil {
                    panic(err)
                } else {
                        fmt.Println("kv updated")
                }
*/


        } else {
            checkErr(err, "Delete failed")
        }

    } else {
        c.JSON(404, gin.H{"error": "policy not found"})
    }

    // curl -i -X DELETE http://localhost:8080/api/v1/policies/common/email
}





