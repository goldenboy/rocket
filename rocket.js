var Rocket = function() {
	
	var	KEY = "key",
		TIMESTAMP = "timestamp",
		TEXT = "text",
		IS_DIRTY = "is_dirty",
		RECEIVE = "receive",
		SEND = "send";
	
	
	
	var settings = {
			ROCKET_URL: "/rocket",
			TABLES: {},
			DATABASE_NAME: "rocket",
			LOG: function(m) {
				// for example: console.log(m);
			},
			LOG_ERROR: function(e) {
				alert(e);
			},
			LOG_SQL: false,
			SYNC_NOW_DELAY_MS: 100
		}, 
		db;

	
	
	var tableTemplate = {
		SYNC_IDLE_TIME_MS: 10000, // sync every 10 seconds
		
		// number of records to load from AppEngine in a single request, reduce this number if requests are using too much CPU cycles or are timing out
		RECEIVE_BATCH_SIZE: 150,		

		// number of records to send to AppEngine in a single request, reduce this number if requests are using too much CPU cycles or are timing out
		SEND_BATCH_SIZE: 150,		
	};
		
	

	function init(_settings, callback) {		
		merge(_settings, settings);

		if (settings.DATABASE) {
			db = settings.DATABASE;
		} else {
			db = openDatabase(settings.DATABASE_NAME, "", "", 1024*1024);
		}
		
		// init tables - setup callback chain
		var initChain = callback?callback:function() {};		
		var tables = settings.TABLES;
		var tablesArray = [];
		for (var tableName in tables) {
			var extendInitChain = function(tableName, currentInitChain) {
				return function() {
					var table = copy(tableTemplate);
					merge(tables[tableName], table);
					tables[tableName] = table;					                  
					table.TABLE_NAME = tableName;
					initTable(table, currentInitChain);
				}
			}
			
			initChain = extendInitChain(tableName, initChain);
		}		

		// trigger init chain
		initChain();
	}
	
	
	
	/**
	 * Helper - merges attributes of one object into another.
	 */
	function merge(from, to) {
		for (attr in from) { 
			to[attr] = from[attr]; 
		}
	}
	
	
	
	/**
	 * Helper - copies (clones) an object.
	 */
	function copy(obj) {
		var c = {};
		
		for (attr in obj) {
			if (obj[attr] && typeof obj[attr] == "object") {
				c[attr] = copy(obj[attr]);
			} else {
				c[attr] = obj[attr];
			}			
		}
		
		return c;
	}

	
	
	/**
	 * Database API helper - implements error handler for "transaction" function
	 */	
	function transaction(callback, successCallback) {
		var errorCallback = function(e) {
			settings.LOG_ERROR("Transaction Error: " + e.message);
		};
		
		if (successCallback) {
			db.transaction(callback, errorCallback, successCallback);
		} else {
			db.transaction(callback, errorCallback);
		}		
	}
	
	
	
	/**
	 * Database API helper - implements error handler for "executeSql" function and optional SQL logging
	 */	
	function executeSql(t, sql, params, callback) {
		if (settings.LOG_SQL) {
			settings.LOG("SQL: " + sql + " [" + params + "]");
		}		

		t.executeSql(sql, params, callback, function(t, e) {
			settings.LOG_ERROR("SQL Error: " + sql + ": [" + params + "]: " + e.message);
		});
	}

	
	
	/**
	 * Initializes table synchronization.
	 */
	function initTable(table, callback) {
		var tableName = table.TABLE_NAME,
			parsedFields = {};
		
		settings.LOG("initTable: " + tableName);
		
		transaction(function(t) {
			
			executeSql(t, "select * from SQLITE_MASTER where TYPE = 'table' and name = ?",  [tableName], function(t, rs) {

		    	if (rs.rows.length) {
		            // table exist - parse columns	                
		    		
		    		var sql = rs.rows.item(0).sql, fields = sql.match(/\(([^\)]*)\)/)[1].split(",");
		    		
		    		for (var i = 0; i < fields.length; i++) {
		    			var field = fields[i].trim().split(" ");
		    			
		    			if (field[0] != IS_DIRTY) {
		    				parsedFields[field[0]] = field[1];
		    			}
		    			
		    			settings.LOG("initTable: added column from table " + tableName + "." + field[0] + "=" + field[1]);
		    		}		    		
		    		
		    	} else {
    	    		settings.LOG("initTable: creating new table " + tableName);
		    		executeSql(t, "create table " + tableName + " (" + KEY + " text primary key, " + IS_DIRTY + " integer default 1)", []);
		    		parsedFields[KEY] = TEXT;
		    	}		    	
		    }); 
			
		}, function() {
			
			transaction(function(t) {
		    	
		    	if (table.FIELDS) {
		    		for (var fieldName in table.FIELDS) {
		    			if (!(fieldName in parsedFields)) {
            				settings.LOG("initTable: adding column to table " + tableName + "." + fieldName + "=" + table.FIELDS[fieldName]);
            				executeSql(t, "alter table " + tableName + " add " + fieldName + " " + table.FIELDS[fieldName]);    		    				
		    			}
		    		}
		    		
		    		merge(table.FIELDS, parsedFields)
		    	}
		    	
		    	table.FIELDS = parsedFields;
        		
			}, function() {
				// success, table structure has been synchronized, now synchronize data
				syncNow(tableName);
				
				// and invoke callback
				callback();
			});			
		});
	}
	
		

	/** 
	 * Performs synchronization for table after the configured delay (SYNC_NOW_DELAY_MS setting).
	 */
	function syncNow(tableName) {
		scheduleSync(tableName, settings.SYNC_NOW_DELAY_MS);
	}


	
	/**
	 * Schedules synchronization for table after the specified period.
	 */
	function scheduleSync(tableName, period) {
		var ctx = "scheduleSync(" + tableName + "," + period + "): ";
				
		var table = settings.TABLES[tableName];
		if (table.syncTimeout) {
			clearTimeout(table.syncTimeout);
		}
		
		if (navigator.onLine) {
			settings.LOG(ctx + "scheduling");
			table.syncTimeout = setTimeout(function() {
				delete table.syncTimeout;
				syncTable(table);
			}, period);
		} else {
			settings.LOG(ctx + "browser is offline, will sync when it's back online");
			
			var sync = function() { 
				settings.LOG(ctx + "browser is now online, syncing");
				syncTable(table);
				window.removeEventListener(sync);
			};
			
			window.addEventListener("online", sync);
		}		
	}
	
	
	
	/**
	 * Synchronizes the given table with AppEngine.
	 *  
	 * Sends updates for the given table to Appengine.
	 * Updates are identified based on is_dirty field.
	 * 
	 * Receives updates for the given table from AppEngine.
	 * Updates are identified based on timestamp field.
	 */
	function syncTable(table) {
		var tableName = table.TABLE_NAME,
			ctx = "syncTable(" + tableName + "): ",
			
			sendBatchSize = table.SEND_BATCH_SIZE,
			sendTotalCount = 0,			
			sendSql = "select * from " + tableName + " where " + IS_DIRTY + " > 0", 
			
			receiveBatchSize = table.RECEIVE_BATCH_SIZE,
			receiveTotalCount = 0,
			receiveState = getState(tableName, RECEIVE),
			
			urlBase = settings.ROCKET_URL + "/" + tableName + "?count=" + receiveBatchSize;
        
		var syncBatch = function() {
			var sendCount;
			var updatesOut = [];
			
			transaction(function(t) {
				executeSql(t, sendSql + " limit " + sendTotalCount + "," + sendBatchSize, [], function(t, rs) {
					sendCount = rs.rows.length;
					sendTotalCount += sendCount;
					
					settings.LOG(ctx + "Sending " + sendCount + " updates");
					
					for (var updatesOutIndex = 0; updatesOutIndex < sendCount; updatesOutIndex++) {
						var update = rs.rows.item(updatesOutIndex);
						settings.LOG(ctx + "Sending update, key: " + update[KEY])
						updatesOut.push(update);
					}
				})					
			}, function() {
				
				// successfully retrieved outbound updates from DB				

				// figure out URL for the current batch
				var url = urlBase;
			    if (receiveState) {
			    	url += "&from=" + receiveState;
			        settings.LOG(ctx + "Receiving records from " + receiveState);
			    } else {
			    	settings.LOG(ctx + "Receiving records from beggining");
			    }			        					
									
			    // post outbound updates to server and parse inbound updates
				$.post(url, {"updates": JSON.stringify(updatesOut)}, function(data) {
					
			    	if (data.updates) {
			    		// all is ok so far, outbound updates have been posted and inbound updates are ready to be processed 
			    		
			    		// mark outbound updates as processed
						transaction(function(t) {
							for (var updatesOutIndex = 0; updatesOutIndex < sendCount; updatesOutIndex++) {
								var update = updatesOut[updatesOutIndex];
								// TODO - probably need some logic to ensure is_dirty doesn't get set below zero
								executeSql(t, "update " + tableName + " set " + IS_DIRTY + " = " + IS_DIRTY + " - ? where key = ?", [update[IS_DIRTY], update[KEY]]);
							}
						}, function() {
	
							// process inbound updates
				        	var updatesIn = data.updates,            		
				    			receiveCount = updatesIn.length;
				    	
				        	receiveTotalCount += receiveCount;
				        	
				        	settings.LOG(ctx + "Received " + receiveCount + " updates");
				        	
				        	// define function that processes inbound updates 1 by 1
				        	var receiveUpdate = function(updateInIndex) {
				        		if (updateInIndex < receiveCount) {
				            		var update = updatesIn[updateInIndex];
				            		
				            		var key = update[KEY];
				            		
				            		settings.LOG(ctx + "Received update, key: " + key);
	
				        			transaction(function(t) {
				                		// check if we need to add any field
				                		for (fieldName in update) {
				                			var value = update[fieldName];
				                			
				                			if (!(fieldName in table.FIELDS) && fieldName != TIMESTAMP) {
				                				var type;
				                				if (typeof value == "number") {
				                					type = "numeric";
				                				} else  {
				                					type = "text";
				                				}
				                					
				                				settings.LOG(ctx + "Adding field " + fieldName + " of type " + type);
				                				executeSql(t, "alter table " + tableName + " add " + fieldName + " " + type);
				                				table.FIELDS[fieldName] = type;
				                			}
				                		}
				                		
				        			}, function() {
	
				        				// transaction succeeded, insert or update values	
				        				
				        				transaction(function(t) {
				        					executeSql(t, "select " + KEY + ", " + IS_DIRTY + " from " + tableName + " where " + KEY + " = ?", [key], function(t, rs) {
				        						var sql, params = [];
				        						if (rs.rows.length > 0) {
				        							// record already exist
	
				        							if (rs.rows.item(0)[IS_DIRTY] > 0) {
				        								settings.LOG(ctx + "Unsynchronized outbound updates exist, skipping inbound update, key: " + key);
				        							} else {
					        							var fields = [];
					        							for (var fieldName in update) {
					        								if (fieldName != KEY && fieldName != TIMESTAMP) {
					        									fields.push(fieldName + "=?");
					        									params.push(update[fieldName]);
					        								}
					        							}
					        							
					        							sql = "update " + tableName + " set " + fields.join(",") + " where " + KEY + "=?";
					        							
					        							params.push(key);
				        							}
				        							
				        						} else {
				        							// new record
				        							
				        							var fields = [IS_DIRTY], values = [0];
				        							for (var fieldName in update) {
				        								if (fieldName != TIMESTAMP) {
					    									fields.push(fieldName);
					    									params.push(update[fieldName]);
					    									values.push("?");
				        								}
				        							}
				        							
				        							sql = "insert into " + tableName + " (" + fields.join(",") + ") values (" + values.join(",") + ")";             							
				        						}
				        						
				        						executeSql(t, sql, params);
				        					});
				        					
				        				}, function() {
				        					// transaction succeeded, update state and process next update
				        					
				        					receiveState = setState(tableName, RECEIVE, update[TIMESTAMP]);
				        					
				        					if (table.ON_RECEIVE) {
				        						// asynchronously call ON_RECEIVE handler
				        						setTimeout(function() {
				        							table.ON_RECEIVE(update);
				        						}, 0);
				        					}
				        					
				                			receiveUpdate(updateInIndex + 1);
				        				});
				            			
				        			});
				        			
				        		} else {
				        			
				        			// all inbound updates processed
				                	
				                    if (receiveCount < receiveBatchSize) {
				                		// looks like there are no more inbound updates
				                    	var batchStartTimestamp = data.timestamp;
				                		if (!receiveState || batchStartTimestamp > receiveState) {
				                			receiveState = setState(tableName, RECEIVE, batchStartTimestamp);
										}			                    		
				                	}            			
	
				                	
				                    if (sendCount == sendBatchSize || receiveCount == receiveBatchSize) {
				                    	// sync next batch
				    	            	syncBatch(); 
				    	            	
				                	} else {
				                		// we are done, schedule next sync cycle
				            			scheduleSync(tableName, table.SYNC_IDLE_TIME_MS);
				            		}  
				        		}
				        	};
				        	
				        	// trigger processing of inbound updates
				        	receiveUpdate(0);  	
						});

					} else {
						settings.LOG(ctx + "Bad response received, browser is probably offline");
						// schedule next sync cycle
						scheduleSync(tableName, table.SYNC_IDLE_TIME_MS);
					}
				});
								
			});
		};
		
		syncBatch();		
	}
	
	
	
	/**
	 * Retrieves synchronization state from localStorage for given table and type (send/receive).
	 */	
	function getState(tableName, type) {
		return localStorage.getItem("rocket__" + tableName + "__" + type);
	}


	
	/**
	 * Save synchronization state to localStorage for given table and type (send/receive).
	 */	
	function setState(tableName, type, state) {
		localStorage.setItem("rocket__" + tableName + "__" + type, state);
		return state;
	}

	

	/**
	 * Reset datatabase (drop all tables) and synchronization state.
	 * Usually should be invoked on logout.
	 */
	function reset(settings) {		
		db = openDatabase(settings.DATABASE_NAME, "", "", 1024*1024);
		
		transaction(function(t) {
			var tables = settings.TABLES;
			for (var i = 0; i < tables.length; i++) {
				var table = tables[i];
				localStorage.removeItem("rocket__" + table + "__" + SEND);
				localStorage.removeItem("rocket__" + table + "__" + RECEIVE);
				executeSql(t, "drop table if exists " + table);
			}
		});
	};
	

	
	/**
	 * UUID generator based on random numbers.
	 * If idPrefix is true, UUID is prefixed with "ID-". This is useful for example for Google App Engine which will 
	 * choke if key name starts with a number. 
	 * Snippet from http://stackoverflow.com/questions/105034/how-to-create-a-guid-uuid-in-javascript/2117523#2117523
	 */
	function generateUUID(idPrefix) {
		// 
		return ((idPrefix?"ID-":"") + ('xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) { 
		    var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
		    return v.toString(16);
		})).toUpperCase());
	}
		

	
	return {
		init: init,
		reset: reset,
		db: db,
		syncNow: syncNow,
		transaction: transaction,
		executeSql: executeSql,
		generateUUID: generateUUID
	};
	
}();