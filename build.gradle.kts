plugins {
    id("java")
    id("application")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.hadoop:hadoop-common:3.3.2")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.2")
    implementation("org.apache.hadoop:hadoop-hdfs:3.3.2")
}

application {
    mainClass.set("org.example.SalesAnalysisDriver")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "org.example.SalesAnalysisDriver"
    }
}