var mysql = require('mysql');
var fs = require('fs');
var Dropbox = require('dropbox');
var moment = require('moment');
var exec = require('node-exec-promise').exec;

function backupCron(config){
    var self = this;
    self.config = config;
    self.connection = {};
    self.dbxInstance = {};
    self.listFolders = [];
    self.today = moment().format('YYYY-MM-DD');
    self.aDatabasesRestricted = [];

    // Public Methods
    self.start = start;

    // Private Methods
    self.loadConfig = loadConfig;
    self.runBackup = runBackup;
    self.loadConnection = loadConnection;
    self.query = query; 
    self.generateDumps = generateDumps;
    self.excludeDatabases = excludeDatabases;
    self.checkIfExistsFolder = checkIfExistsFolder;
 
    // Dropbox related methods
    self.authenticateDropbox = authenticateDropbox;    
    self.createDropboxFolder = createDropboxFolder;
    self.performUpload = performUpload;
    self.startUploadSession = startUploadSession;
    self.appendUploadSession = appendUploadSession;
    self.finishUploadSessions = finishUploadSessions;
    self.prepareSessionFileInfo = prepareSessionFileInfo;
    self.uploadFileToDropbox = uploadFileToDropbox;
    self.divideFilebyChunks = divideFilebyChunks;
    
    function start(){
        self.loadConfig();
        self.runBackup();
    }
  
    function loadConfig(){
        // Cargamos configuración del .json
        var conf = fs.readFileSync(__dirname + '/config.json', 'utf8');
        self.config = JSON.parse(conf);
        self.loadConnection();            
    }
  
    function loadConnection(){
        // Cargamos conexión mysql
        self.connection = mysql.createConnection(self.config.mysql);
        self.connection.connect();
    }

    function runBackup(){
        // Traemos la listas de db del servidor
        self.query('show databases')
            .then(function(res){
                self.generateDumps(res);
            })
            .catch(function(err){
                console.log("Cannot read databases",err);
            });        
    }

    function query(query){
        return new Promise(function(resolve,reject){
            self.connection.query(query, function (error, results, fields) {
                if (error) 
                    reject(error);
                    
                    resolve(results);
            });

            self.connection.end();  
        })      
    }

    function generateDumps(aDatabases){
        // Generamos los dump files de cada base de datos
        self.aDatabasesRestricted = self.excludeDatabases(aDatabases);
        var iFinishedPromises = 0;

        aDatabasesRestricted.forEach(function(element){
            exec(self.config.dumpPath + ' -u '+ self.config.mysql.user +'-p '+ self.config.mysql.password  +' '+ element +' > '+ element +'.sql')
                .then(function(result){
                    iFinishedPromises++;

                    // Si finalizan todas las promesas
                    if (aDatabasesRestricted.length == iFinishedPromises)
                        self.authenticateDropbox();
                })
                .catch(function(err){
                    console.log(err);
                })

        })        
    }

    function excludeDatabases(aDatabases){
        // Excluímos base de datos que no cumplen con las especificaciones del config.json
        aFilteredDatabases = aDatabases.filter(checkIfIsExcluded)
                                       .map(removeDatabaseFormat);
                                    
        return aFilteredDatabases;

        function checkIfIsExcluded(element){
            if(self.config.db.exclude.indexOf(element.Database) == -1){
                return element;  
            }                  
        }

        function removeDatabaseFormat(element){
            return element.Database;
        }
    }

    function authenticateDropbox(){
        // Authenticamos con dropbox ( Usando un access token generado para mi propia cuenta)
        self.dbxInstance = new Dropbox({ accessToken: self.config.dropbox.accessToken });

        // Listamos directorios
        self.dbxInstance.filesListFolder({path: ''})
        .then(function(response) {
            self.listFolders = response;
            if(self.checkIfExistsFolder(self.config.dropbox.folderName))
                self.createDropboxFolder('/' + self.config.dropbox.folderName + '/' + self.today)
                    .then(function(res){
                          self.performUpload();                 
                    });      
            else{
                self.createDropboxFolder('/'+ self.config.dropbox.folderName)
                    .then(function(res){
                        self.createDropboxFolder('/' + self.config.dropbox.folderName + '/' + self.today)
                            .then(function(res){
                                self.performUpload();                 
                            });                         
                    })
                    .catch(function(err){
                        console.log("Error",err)
                    })
            }
                
            /*;*/            
        })
        .catch(function(error) {
            console.log(error);
        }); 
    }

    function checkIfExistsFolder(folderName){
        var boolExists = false;

        if(!self.listFolders.entries)
            return boolExists;

        boolExists = self.listFolders.entries.some(function(el,ind,arr){
            if(el.name == folderName)
                return true;
        })

        return boolExists;
    }

    function prepareSessionFileInfo(aFiles){
        // Parameter session vars
        var oFilesCommitInfo = {};
        var oFilesUploadSessionCursor = {};
        var oFinishBatchObject = {};
        var oFinalObject = {entries : [] };

        var ret = aFiles.map(function(el,ind,arr){
            oFinalObject.entries.push(makeFinishBatchObject(el));
        })

        return oFinalObject;

        function makeFinishBatchObject(el){
            oFilesCommitInfo = {
                                    path :  '/' + self.config.dropbox.folderName + '/' + self.today + '/' + el.fileInfo, 
                                    mode :  { '.tag' : 'overwrite' }, 
                                    autorename : false, 
                                    mute : false };
            oFilesUploadSessionCursor = { session_id: el.session_id , offset: el.offset };  
            oFinishBatchObject = {cursor: oFilesUploadSessionCursor, commit: oFilesCommitInfo};
            return oFinishBatchObject;
        }
    }

    function performUpload(){
        var aPromises = [];
        self.aDatabasesRestricted.forEach(function(el){
            aPromises.push(self.startUploadSession(el))
        });

        // Ejecutamos las promises que llaman al metodo que inicia session de subida de archivos 
        Promise.all(aPromises)
        .then(function(res){
                var aFileSessionObjects = prepareSessionFileInfo(res);

                finishUploadSessions(aFileSessionObjects)
                    .then(function(res){
                        console.log("SUCCESS");
                    })
                    .catch(function(err){
                        console.log("FAILED")
                    })

        })
        .catch(function(err){
            console.log("Error uploading files to dropbox",err);
        })           
    }

    function divideFilebyChunks(fileInfo){
        var myReadStream = fs.createReadStream(fileInfo,'utf8');
        var aChunks = [];
        var offset = 0;

        return new Promise(function(resolve,reject){
            myReadStream.on('readable',function(){
                var chunk;
                var byteSize = self.config.chunkSizesInBytes;
                while ( (chunk = myReadStream.read(byteSize)) ) {                                         
                    aChunks.push({ chunkContent : chunk , sizeSumarized : offset});
                    offset += Buffer.byteLength(chunk);                                                         
                }

                resolve(aChunks);                
            })
        })
    }


    function createDropboxFolder(folderName){
        //console.log("Carpeta",folderName);
        return new Promise(function(resolve,reject){
            self.dbxInstance.filesCreateFolder({path : folderName , autorename: true})
                            .then(function(res){
                                resolve(res);
                            })
                            .catch(function(err){
                                reject(err);
                            });
        })
    }

    function startUploadSession(fileName){
       var fileInfo = fileName +'.sql';        
        return new Promise(function(resolve,reject){
            fs.readFile(fileInfo, 'utf8', function(err, data) {  
                    if (err) throw err;
                    var oArg = { contents : data , 
                                 close : true };
                    var stats = fs.statSync(fileInfo);
                    var fileSize = stats.size;
                    var fileSizeInMb = fileSize / 1000000.0;
                    var aPromises = [];
                    var aFileUploadDivided = [];

                    // Si el peso es mayor a 150 megas, el archivo se envía por partes
                    if (fileSizeInMb > 150){
                        self.divideFilebyChunks(fileInfo)
                            .then(function(res){
                                aFileUploadDivided = res;
                                var firstChunk = aFileUploadDivided.shift();
                                var oArg = { contents : firstChunk.chunkContent , 
                                            close : false };                                

                                // Iniciamos sessión con el primer chunk del archivo
                                return  self.dbxInstance.filesUploadSessionStart(oArg)
                            })
                            .then(function(res){
                                var bClose = false;

                                // Preparamos parámetros para cada parte del archivo que se pasara al metodo append
                                aFileUploadDivided.forEach(function(el,ind,arr){
                                    if (ind == arr.length-1)
                                        bClose = true;
                                    
                                    var oFilesUploadSessionAppend = {   contents: el.chunkContent,
                                                                        cursor : {
                                                                                    session_id : res.session_id,
                                                                                    offset: el.sizeSumarized },
                                                                        close : bClose };                                      
                                    
                                    aPromises.push(oFilesUploadSessionAppend);
                                });

                                // Ejecutamos secuencialmente las promesas para no perder sincronía con el tamaño de subida
                                sequentialAsyncWithReduce(aPromises)
                                    .then(function(res){
                                        res[res.length - 1].fileInfo = fileInfo;
                                        res[res.length - 1].session_id = res[res.length - 1].cursor.session_id;
                                        res[res.length - 1].offset = fileSize;
                                        resolve(res[res.length - 1]);
                                    })
                                    .catch(function(err){
                                        console.log("ERROR APENDING CHUNKS FILES",err);
                                    });

                                    
                                
                            })
                    }else{
                        self.dbxInstance.filesUploadSessionStart(oArg)
                                        .then(function(res){
                                            console.log("File upload session started succesfully");
                                            res.offset = fileSize;
                                            res.fileInfo = fileInfo;

                                            resolve(res);
                                        })
                                        .catch(function(err){
                                            console.log("ERROR : File upload session doesn't start",err);
                                            reject("ERR",err);
                                        });
                    }



            })  
        })
    }

    function appendUploadSession(oFilesUploadSessionAppend){
        return new Promise(function(resolve,reject){         
            self.dbxInstance.filesUploadSessionAppendV2(oFilesUploadSessionAppend)
                            .then(function(res){
                                resolve(oFilesUploadSessionAppend);                                      
                            })
                            .catch(function(err){
                                console.log("Error appending a chunk of a file",err.error);
                                reject("ERR",err);
                            });
        })
    }

    function finishUploadSessions(aFileSessionObjects){
        return new Promise(function(resolve,reject){
            self.dbxInstance.filesUploadSessionFinishBatch(aFileSessionObjects)
                            .then(function(res){
                                console.log("Upload Sessions Finished",res);
                                self.dbxInstance.filesUploadSessionFinishBatchCheck({ async_job_id : res.async_job_id })
                                    .then(function(res){
                                        console.log("Batch session status", res);
                                    });
                            })
                            .catch(function(err){
                                console.log("Error finishing sessions",err);
                            });
        })
    }

    function uploadFileToDropbox(fileName,path){
        var fileInfo = fileName +'.sql';
        var destinationFilePath = '/' + self.config.dropbox.folderName + '/' + path + '/' + fileInfo;

        return new Promise(function(resolve,reject){
            fs.readFile(fileInfo, 'utf8', function(err, data) {  
                    if (err) throw err;
                    var oFilesCommitInfo = {contents : data , 
                                        path :  destinationFilePath, 
                                        mode :  { '.tag' : 'overwrite' }, 
                                        autorename : false, 
                                        mute : false }


                    self.dbxInstance.filesUpload(oFilesCommitInfo)
                                    .then(function(res){
                                        console.log("File upload Successfuly");
                                        resolve("SATISFIED",res);
                                    })
                                    .catch(function(err){
                                        console.log("Error uploading a file",err);
                                        reject("ERR",err);
                                    });
            })  
        })
    }

    function sequentialAsyncWithReduce (arr) {
        var values = arr;
        var newValues = [];
        
        return new Promise(function(resolve,reject){
            return values.reduce(function(memo, value) {
                return memo.then(function() {
                    //console.log("Entramos",value);
                    return self.appendUploadSession(value);
                }).then(function(newValue) {
                    // ...
                    newValues.push(newValue);
                })
                .catch(function(err){
                    console.log("Error appending chunk files", err);
                    reject(err);
                })
            }, Promise.resolve(null)).then(function() {
                resolve(newValues);
                return newValues;
            });
        })

    }    

    return {
        start: self.start
    }

}

module.exports = backupCron;

