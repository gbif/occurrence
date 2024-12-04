pipeline {
  agent any
  tools {
    maven 'Maven 3.8.5'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '5'))
    skipStagesAfterUnstable()
    timestamps()
  }
   parameters {
    separator(name: "release_separator", sectionHeader: "Release Main Project Parameters")
    booleanParam(name: 'RELEASE', defaultValue: false, description: 'Do a Maven release')
    string(name: 'RELEASE_VERSION', defaultValue: '', description: 'Release version (optional)')
    string(name: 'DEVELOPMENT_VERSION', defaultValue: '', description: 'Development version (optional)')
    booleanParam(name: 'DRY_RUN_RELEASE', defaultValue: false, description: 'Dry Run Maven release')

    separator(name: "release_separator", sectionHeader: "Release Trino UDFs Parameters")
    booleanParam(name: 'RELEASE_TRINO', defaultValue: false, description: 'Do a Maven release')
    string(name: 'RELEASE_VERSION_TRINO', defaultValue: '', description: 'Release version (optional)')
    string(name: 'DEVELOPMENT_VERSION_TRINO', defaultValue: '', description: 'Development version (optional)')
    booleanParam(name: 'DRY_RUN_RELEASE_TRINO', defaultValue: false, description: 'Dry Run Maven release')
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
          not { expression { params.RELEASE_TRINO } };
        }
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS'),
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.custom.CustomConfig1389220396351', variable: 'APPKEYS_TESTFILE')
          ]) {
          sh 'mvn -s ${MAVEN_SETTINGS} clean deploy -Denforcer.skip=true -T 1C -Dparallel=classes -DuseUnlimitedThreads=true -Pgbif-dev -U -Djetty.port=${JETTY_PORT} -Dappkeys.testfile=${APPKEYS_TESTFILE} -B'
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
          not { expression { params.RELEASE_TRINO } };
        }
      }
      steps {
        configFileProvider([
            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')
          ]) {
          // occurrence-trino-udf needs jdk17 because the trino library uses jdk17
          sh '''
            cd occurrence-trino-udf
            mvn -s ${MAVEN_SETTINGS} clean deploy -Pgbif-dev -U -B
          '''
        }
      }
    }

    stage('Build and push Docker images: Downloads') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.RELEASE_TRINO } };
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
          not { expression { params.RELEASE_TRINO } };
        }
      }
      steps {
        sh '''
          build/occurrence-table-build-spark-docker-build.sh $(mvn -q -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec)
        '''
      }
    }

    stage('Maven release: Main project (Java 11)') {
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
          RELEASE_ARGS = createReleaseArgs(params.RELEASE_VERSION, params.DEVELOPMENT_VERSION, params.DRY_RUN_RELEASE)
      }
      steps {
          configFileProvider(
                  [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                          variable: 'MAVEN_SETTINGS_XML')]) {
              git 'https://github.com/gbif/occurrence.git'
              sh 'mvn -s $MAVEN_SETTINGS_XML -B -Denforcer.skip=true release:prepare release:perform $RELEASE_ARGS'
          }
      }
    }

    stage('Maven release: Trino module (Java 17) ') {
      tools {
        jdk 'OpenJDK17'
      }
      when {
          allOf {
              expression { params.RELEASE_TRINO };
              branch 'master';
          }
      }
      environment {
          RELEASE_ARGS_TRINO = createReleaseArgsTrino(params.RELEASE_VERSION_TRINO, params.DEVELOPMENT_VERSION_TRINO, params.DRY_RUN_RELEASE_TRINO)
      }
      steps {
          configFileProvider(
                  [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                          variable: 'MAVEN_SETTINGS_XML')]) {
              git 'https://github.com/gbif/occurrence.git'
              sh '''
                cd occurrence-trino-udf
                mvn -s $MAVEN_SETTINGS_XML -B release:prepare release:perform $RELEASE_ARGS_TRINO
              '''
          }
      }
    }

     stage('Docker Release: Downloads') {
       when {
        allOf {
          expression { params.RELEASE };
          branch 'master';
        }
      }
      environment {
          VERSION = getReleaseVersion(params.RELEASE_VERSION)
      }
      steps {
        sh 'build/occurrence-download-spark-docker-build.sh ${VERSION}'
      }
     }

     stage('Docker Release: Table build') {
      when {
        allOf {
          expression { params.RELEASE };
          branch 'master';
        }
      }
      environment {
          VERSION = getReleaseVersion(params.RELEASE_VERSION)
      }
      steps {
        sh 'build/occurrence-table-build-spark-docker-build.sh ${VERSION}'
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

def createReleaseArgs(inputVersion, inputDevVersion, inputDryrun) {
    def args = ""
    if (inputVersion != '') {
        args += " -DreleaseVersion=" + inputVersion
    }
    if (inputDevVersion != '') {
        args += " -DdevelopmentVersion=" + inputDevVersion
    }
    if (inputDryrun) {
        args += " -DdryRun=true"
    }

    return args
}

def getReleaseVersion(inputVersion) {
    if (inputVersion != '') {
        return inputVersion
    }
    return """${sh(returnStdout: true, script: '$(mvn -q -Dexec.executable=\'echo\' -Dexec.args=\'${project.version}\' --non-recursive exec:exec)')}"""
}
