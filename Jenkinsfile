@Library('gbif-common-jenkins-pipelines') _

pipeline {
  agent any
  tools {
    maven 'Maven 3.8.5'
    jdk 'OpenJDK17'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '5'))
    skipStagesAfterUnstable()
    timestamps()
  }
  environment {
    JETTY_PORT = utils.getPort()
    POM_VERSION = readMavenPom().getVersion()
  }
  stages {

    stage('Maven build: mini cluster module (Java 17)') {
      tools {
        jdk 'OpenJDK17'
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')
          ]) {
          // occurrence-table-build-trino needs jdk17 because the trino library uses jdk17
          sh 'mvn -s ${MAVEN_SETTINGS} clean deploy -pl occurrence-hadoop-minicluster'
        }
      }
    }

    stage('Maven build: Main project (Java 17)') {
      tools {
        jdk 'OpenJDK17'
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS'),
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.custom.CustomConfig1389220396351', variable: 'APPKEYS_TESTFILE')
          ]) {
          sh 'mvn -s ${MAVEN_SETTINGS} clean deploy -Denforcer.skip=true -T 1C -Dparallel=classes -DuseUnlimitedThreads=true -Pgbif-dev -U -Djetty.port=${JETTY_PORT} -Dappkeys.testfile=${APPKEYS_TESTFILE} -B -pl \'!occurrence-hadoop-minicluster\''
        }
      }
    }

    stage('Build and push Docker images: Downloads') {
      environment {
          VERSION = utils.getReleaseVersion(params.RELEASE_VERSION, POM_VERSION)
      }
      steps {
        sh 'build/occurrence-download-spark-docker-build.sh 1.0.23-multitaxonomy-SNAPSHOT'
      }
    }

    stage('Build and push Docker images: Table build') {
      environment {
          VERSION = utils.getReleaseVersion(params.RELEASE_VERSION, POM_VERSION)
      }
      steps {
        sh 'build/occurrence-table-build-spark-docker-build.sh 1.0.23-multitaxonomy-SNAPSHOT'
      }
    }
  }

    post {
      success {
        echo 'Pipeline executed successfully!'
      }
      failure {
        echo 'Pipeline execution failed!'
    }
  }
}

def getPort() {
  try {
      return new ServerSocket(0).getLocalPort()
  } catch (IOException ex) {
      System.err.println("no available ports");
  }
}
