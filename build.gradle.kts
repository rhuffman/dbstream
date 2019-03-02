plugins {
    `java-library`
    idea
    groovy
}

repositories {
    jcenter()
}

dependencies {
    api("org.apache.commons:commons-math3:3.6.1")

    implementation("com.google.guava:guava:26.0-jre")

    testImplementation("org.codehaus.groovy:groovy-all:2.5.4")
    testImplementation("org.spockframework:spock-core:1.2-groovy-2.5")
    testImplementation("junit:junit:4.12")
}
