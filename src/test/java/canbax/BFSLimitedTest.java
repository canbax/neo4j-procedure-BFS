package canbax;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.driver.v1.StatementResult;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BFSLimitedTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;
    // Neo4j generates ids of nodes and starting from 0 and increases them one by one
    private final String sampleGraphCql = "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
            + "CREATE (n14:Person {name:'n14'}) CREATE "
            + "(n1)-[:KNOWS]->(n2), (n1)-[:KNOWS]->(n3)," // edges from root to 1. level
            + "(n2)-[:KNOWS]->(n4), (n2)-[:KNOWS]->(n5),(n3)-[:KNOWS]->(n6), (n3)-[:KNOWS]->(n7)," // edges from 1. level to 2. level
            + "(n4)-[:KNOWS]->(n8), (n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10), (n6)-[:KNOWS]->(n11)," // edges from 2. level to 3 level
            + "(n11)-[:KNOWS]->(n12),(n11)-[:KNOWS]->(n13),(n11)<-[:KNOWS]-(n14);"; // edges from 3. level to 4. level
// CALL apoc.cypher.runFile("file:///sub2.txt") // to run a cypher script file
//    CALL apoc.export.cypher.query("match (n)-[r]->(n2) return * limit 100", "subset.cypher",
//    {format:'plain',separateFiles:false, cypherFormat: 'create', useOptimizations:{type: "NONE", unwindBatchSize: 20}})
//    YIELD file, batches, source, format, nodes, relationships, time, rows, batchSize
//    RETURN file, batches, source, format, nodes, relationships, time, rows, batchSize;

    @BeforeEach
    void initializeNeo4j() {
        this.embeddedDatabaseServer = TestServerBuilders.newInProcessBuilder().withProcedure(BFSLimited.class)
                .newServer();
    }

    /**
     * test for getting depth 3 undirected fashion
     */
    @Test
    public void test1() {

        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
             Session session = driver.session()) {
            // And given I have a node in the database
            session.run(sampleGraphCql);

            // find 1 common downstream of 3 nodes
            StatementResult result = session.run("CALL BFS([0], 3, false) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            ArrayList<Long> trueNodeSet = new ArrayList<>();
            for (long i = 0L; i < 11L; i++) {
                trueNodeSet.add(i);
            }
            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
        }
    }

    /**
     * test for getting depth 4 undirected fashion
     */
    @Test
    public void test2() {

        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
             Session session = driver.session()) {
            // And given I have a node in the database
            session.run(sampleGraphCql);

            // find 1 common downstream of 3 nodes
            StatementResult result = session.run("CALL BFS([0], 4, false) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            ArrayList<Long> trueNodeSet = new ArrayList<>();
            for (long i = 0L; i < 14L; i++) {
                trueNodeSet.add(i);
            }
            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
        }
    }

    /**
     * test for getting depth 4 directed fashion
     */
    @Test
    public void test3() {

        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
             Session session = driver.session()) {
            // And given I have a node in the database
            session.run(sampleGraphCql);

            // find 1 common downstream of 3 nodes
            StatementResult result = session.run("CALL BFS([0], 4, false) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            ArrayList<Long> trueNodeSet = new ArrayList<>();
            for (long i = 0L; i < 13L; i++) {
                trueNodeSet.add(i);
            }
            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
        }
    }

    @Test
    public void testOnRunningInstance() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver drv = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123"));
             Session session = drv.session()) {

            // find 1 common downstream of 3 nodes
            StatementResult result = session
                    .run("CALL graphOfInterest([5,7], [], 3, false, 100, 1, null, false, null, 2) YIELD nodes, edges return nodes, edges");

            InternalNode n = (InternalNode) result.single().get("nodes").asList().get(0);
//            assertThat(n.id()).isEqualTo(5);
        }
    }

}
