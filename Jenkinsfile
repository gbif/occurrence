pipeline {
  agent any
  tools {
    maven 'Maven3.6'
    jdk 'OpenJDK11'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    skipStagesAfterUnstable()
    timestamps()
  }

  stages {

    stage('Maven build') {
      steps {
        sh 'mvn clean verify -T 1C -Dparallel=classes -DuseUnlimitedThreads=true -Pgbif-dev -U -Djetty.port=$HTTP_PORT -Dappkeys.testfile=$APPKEYS_TESTFILE'
      }
    }

    stage('Snapshots to nexus') {
      steps {
        configFileProvider([configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')]) {
          sh 'mvn -s $MAVEN_SETTINGS deploy -B -Pgbif-dev -DskipTests'
        }
      }
    }

    stage('Build and push Docker images: Downloads') {
      steps {
        sh 'build/occurrence-download-spark-docker-build.sh'
      }
    }

    stage('Build and push Docker images: Table build') {
      when {
        expression {
          params.TYPE == 'FULL'
        }
      }
      steps {
        sh 'build/occurrence-table-build-spark-docker-build.shh'
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
