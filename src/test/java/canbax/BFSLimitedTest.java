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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BFSLimitedTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;


    @BeforeEach
    void initializeNeo4j() {
        this.embeddedDatabaseServer = TestServerBuilders.newInProcessBuilder().withProcedure(BFSLimited.class)
                .newServer();
    }

    @Test
    public void test1() {

        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
             Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                            + "CREATE (n14:Person {name:'n14'}) CREATE "
                            + "(n1)-[:KNOWS]->(n2), (n1)-[:KNOWS]->(n3)," // edges from root to 1. level
                            + "(n2)-[:KNOWS]->(n4), (n2)-[:KNOWS]->(n5),(n3)-[:KNOWS]->(n6), (n3)-[:KNOWS]->(n7)," // edges from 1. level to 2. level
                            + "(n4)-[:KNOWS]->(n8), (n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10), (n6)-[:KNOWS]->(n11)," // edges from 2. level to 3 level
                            + "(n11)-[:KNOWS]->(n12),(n11)-[:KNOWS]->(n13),(n11)-[:KNOWS]->(n14);"); // edges from 3. level to 4. level

            // find 1 common downstream of 3 nodes
            StatementResult result = session
                    .run("CALL BFS([0], 3, false) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            Set<Long> edgeSet = r.get("edges").asList().stream().map(x -> ((InternalRelationship) x).id())
                    .collect(Collectors.toSet());

            ArrayList<Long> trueNodeSet = new ArrayList<>();
            for (long i = 0L; i < 11L; i++) {
                trueNodeSet.add(i);
            }

            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
        }
    }

}
