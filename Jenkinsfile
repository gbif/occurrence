pipeline {
  agent any
  tools {
    maven 'Maven3.6'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    skipStagesAfterUnstable()
    timestamps()
  }
  environment {
    JETTY_PORT = getPort()
  }
  stages {

    stage('Maven build: Main project (Java 11)') {
      tools {
        jdk 'OpenJDK11'
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS'),
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.custom.CustomConfig1389220396351', variable: 'APPKEYS_TESTFILE')
          ]) {
          sh 'mvn -s ${MAVEN_SETTINGS} clean deploy -T 1C -Dparallel=classes -DuseUnlimitedThreads=true -Pgbif-dev -U -Djetty.port=${JETTY_PORT} -Dappkeys.testfile=${APPKEYS_TESTFILE} -B -pl !occurrence-table-build-trino,!occurrence-ws,!occurrence-download-launcher,!occurrence-cli'
        }
      }
    }

    stage('Maven build: Spring modules (Java 17)') {
      tools {
        jdk 'OpenJDK17'
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')
          ]) {
          sh 'mvn -s ${MAVEN_SETTINGS} clean deploy -Pgbif-dev -U -B -pl occurrence-ws,occurrence-download-launcher,occurrence-cli'
        }
      }
    }
    stage('Maven build: Trino module (Java 18)') {
      tools {
        jdk 'OpenJDK18'
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')
          ]) {
          sh 'mvn -s ${MAVEN_SETTINGS} clean deploy -Pgbif-dev -U -B -pl occurrence-table-build-trino'
        }
      }
    }

    stage('Build and push Docker images: Downloads') {
      steps {
        sh 'build/occurrence-download-spark-docker-build.sh'
      }
    }

    stage('Build and push Docker images: Table build') {
      steps {
        sh 'build/occurrence-table-build-spark-docker-build.sh'
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
