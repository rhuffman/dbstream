plugins {
    `java-library`
    idea
    groovy
}

repositories {
    jcenter()
}

dependencies {
    api("commons-dbutils:commons-dbutils:1.7")

    testImplementation("org.codehaus.groovy:groovy-all:2.5.4")
    testImplementation("org.spockframework:spock-core:1.2-groovy-2.5")
    testImplementation("junit:junit:4.12")
}
