'use strict';

/***
 * Exported functions to be used in the testing scripts.
 */
module.exports = {
  uploadRandomizedUser,
  processRegisterReply,
  processDeleteReply,
  processCreateShortReply,
  getShortIdsUser,
  processGetShortsReply,
  processFollowersReply,
  processCreateBlobsReply,
  processGetBlobsReply,
  getRandomExistingShort
}


const fs = require('fs') // Needed for access to blobs.

var registeredUsers = []
var registeredShorts = []
var deletedShorts = []
var registeredFollowings = []
var shortsFound = []
var images = []

// All endpoints starting with the following prefixes will be aggregated in the same for the statistics
var statsPrefix = [ ["/rest/media/","GET"],
			["/rest/media","POST"]
	]

// Function used to compress statistics
global.myProcessEndpoint = function( str, method) {
	var i = 0;
	for( i = 0; i < statsPrefix.length; i++) {
		if( str.startsWith( statsPrefix[i][0]) && method == statsPrefix[i][1])
			return method + ":" + statsPrefix[i][0];
	}
	return method + ":" + str;
}

// Returns a random username constructed from lowercase letters.
function randomUsername(char_limit){
    const letters = 'abcdefghijklmnopqrstuvwxyz';
    let username = '';
    let num_chars = Math.floor(Math.random() * char_limit);
    for (let i = 0; i < num_chars; i++) {
        username += letters[Math.floor(Math.random() * letters.length)];
    }
    return username;
}


// Returns a random password, drawn from printable ASCII characters
function randomPassword(pass_len){
    const skip_value = 33;
    const lim_values = 94;
    
    let password = '';
    let num_chars = Math.floor(Math.random() * pass_len);
    for (let i = 0; i < pass_len; i++) {
        let chosen_char =  Math.floor(Math.random() * lim_values) + skip_value;
        if (chosen_char == "'" || chosen_char == '"')
            i -= 1;
        else
            password += chosen_char
    }
    return password;
}

/**
 * Process reply of the user registration.
 */
function processRegisterReply(requestParams, response, context, ee, next) {
    if( typeof response.body !== 'undefined' && response.body.length > 0) {
        registeredUsers.push(response.body);
    }
    return next();
} 

/**
 * Register a random user.
 */

function uploadRandomizedUser(requestParams, context, ee, next) {
    let username = randomUsername(10);
    let pword = randomPassword(15);
    let email = username + "@campus.fct.unl.pt";
    let displayName = username;
    
    const user = {
        id: username,
        pwd: pword,
        email: email,
        displayName: username
    };
    requestParams.body = JSON.stringify(user);
    return next();
} 


/**
 * Process reply of the creation of the short.
 */
function processCreateShortReply(requestParams, response, context, ee, next) {
    if( typeof response.body !== 'undefined' && response.body.length > 0) {
        registeredShorts.push(response.body);
    }
    return next();
}


/**
 * Use the id of a registered short.
 */
function getShortIdsUser(requestParams, context, ee, next) {
    let short = registeredShorts.pop();
    requestParams.qs = short;
    // requestParams.qs = JSON.stringify(registeredShorts.pop());
    return next();
} 

/**
 * Process reply of the get of the short.
 */
function processGetShortsReply(requestParams, response, context, ee, next) {
    if( typeof response.body !== 'undefined' && response.body.length > 0) {
        shortsFound.push(response.body);
    }
    return next();
} 

/**
 * Process reply of the deletion of the short.
 */
function processDeleteReply(requestParams, response, context, ee, next) {
    if( typeof response.body !== 'undefined' && response.body.length > 0) {
        deletedShorts.push(response.body);
    }
    return next();
}

/**
 * Process reply of the list of the followers.
 */
function processFollowersReply(requestParams, response, context, ee, next) {
    if( typeof response.body !== 'undefined' && response.body.length > 0) {
        registeredFollowings.push(response.body);
    }
    return next();
} 

/**
 * Process reply of the creation of the blob.
 */
function processCreateBlobsReply(requestParams, response, context, ee, next) {
    if( typeof response.body !== 'undefined' && response.body.length > 0) {
        registeredBlobs.push(response.body);
    }
    return next();
}

/**
 * Process reply of the get of the blob.
 */
function processGetBlobsReply(requestParams, response, context, ee, next) {
    if( typeof response.body !== 'undefined' && response.body.length > 0) {
        blobsFound.push(response.body);
    }
    return next();
}

/**
 * Get random short registered in the system.
 */
function getRandomExistingShort(requestParams, response, context, ee, next) {
    let registeredShortsSize = registeredShorts.length;
    let num_chars = Math.floor(Math.random() * registeredShortsSize);
    context.vars.shortId = registeredShortsSize[num_chars];
    return next();
}