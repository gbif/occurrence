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
   parameters {
    separator(name: "release_separator", sectionHeader: "Release Parameters")
    booleanParam(name: 'RELEASE', defaultValue: false, description: 'Do a Maven release')
    string(name: 'RELEASE_VERSION', defaultValue: '', description: 'Release version (optional)')
    string(name: 'DEVELOPMENT_VERSION', defaultValue: '', description: 'Development version (optional)')
    booleanParam(name: 'DRY_RUN_RELEASE', defaultValue: false, description: 'Dry Run Maven release')
  }
  environment {
    JETTY_PORT = getPort()
  }
  stages {

    stage('Maven build: Main project (Java 11)') {
      tools {
        jdk 'OpenJDK11'
      }
       when {
        allOf {
          not { expression { params.RELEASE } };
        }
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS'),
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.custom.CustomConfig1389220396351', variable: 'APPKEYS_TESTFILE')
          ]) {
          sh 'mvn -s ${MAVEN_SETTINGS} clean deploy -T 1C -Dparallel=classes -DuseUnlimitedThreads=true -Pgbif-dev -U -Djetty.port=${JETTY_PORT} -Dappkeys.testfile=${APPKEYS_TESTFILE} -B -pl !occurrence-table-build-trino'
        }
      }
    }

    stage('Maven build: Trino module (Java 17)') {
      tools {
        jdk 'OpenJDK17'
      }
      when {
        allOf {
          not { expression { params.RELEASE } };
        }
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')
          ]) {
          // occurrence-table-build-trino needs jdk17 because the trino library uses jdk17
          sh 'mvn -s ${MAVEN_SETTINGS} clean deploy -Pgbif-dev -U -B -pl occurrence-table-build-trino'
        }
      }
    }

    stage('Build and push Docker images: Downloads') {
      when {
        allOf {
          not { expression { params.RELEASE } };
        }
      }
      steps {
         sh '''
          build/occurrence-download-spark-docker-build.sh $(mvn -q -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec)
         '''
      }
    }

    stage('Build and push Docker images: Table build') {
      when {
        allOf {
          not { expression { params.RELEASE } };
        }
      }
      steps {
        sh '''
          build/occurrence-table-build-spark-docker-build.sh $(mvn -q -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec)
        '''
      }
    }

    stage('Maven release (Java 11)') {
      tools {
        jdk 'OpenJDK11'
      }
      when {
          allOf {
              expression { params.RELEASE };
              branch 'master';
          }
      }
      environment {
          RELEASE_ARGS = createReleaseArgs()
      }
      steps {
          configFileProvider(
                  [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                          variable: 'MAVEN_SETTINGS_XML')]) {
              git 'https://github.com/gbif/vocabulary.git'
              sh 'mvn -s $MAVEN_SETTINGS_XML -B release:prepare release:perform -pl !occurrence-table-build-trino $RELEASE_ARGS'
          }
      }
    }

    stage('Trino module (Java 17) Maven release') {
      tools {
        jdk 'OpenJDK17'
      }
      when {
          allOf {
              expression { params.RELEASE };
              branch 'master';
          }
      }
      environment {
          RELEASE_ARGS = createReleaseArgs()
      }
      steps {
          configFileProvider(
                  [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                          variable: 'MAVEN_SETTINGS_XML')]) {
              git 'https://github.com/gbif/vocabulary.git'
              sh 'mvn -s $MAVEN_SETTINGS_XML -B release:prepare release:perform -pl occurrence-table-build-trino $RELEASE_ARGS'
          }
      }
    }

     stage('Release Docker images: Downloads') {
       when {
        allOf {
          expression { params.RELEASE };
          branch 'master';
        }
      }
      steps {
        sh 'build/occurrence-download-spark-docker-build.sh ${params.RELEASE_VERSION}'
      }
     }

     stage('Release Docker images: Table build') {
      when {
        allOf {
          expression { params.RELEASE };
          branch 'master';
        }
      }
      steps {
        sh 'build/occurrence-table-build-spark-docker-build.sh ${params.RELEASE_VERSION}'
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

def createReleaseArgs() {
    def args = ""
    if (params.RELEASE_VERSION != '') {
        args += "-DreleaseVersion=${params.RELEASE_VERSION} "
    }
    if (params.DEVELOPMENT_VERSION != '') {
        args += "-DdevelopmentVersion=${params.DEVELOPMENT_VERSION} "
    }
    if (params.DRY_RUN_RELEASE) {
        args += "-DdryRun=true"
    }

    return args
}
