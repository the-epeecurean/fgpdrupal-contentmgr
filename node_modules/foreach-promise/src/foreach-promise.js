module.exports = function(array, callback){
    
    if(!array){
        throw "Missing argument 'array'";
    }
    
    let length = array.length;

    if(length === 0){
        throw "Array must have at least one element";
    }
    
    return new Promise( (resolve, reject) =>{
        const promises = getPromises();
        array.forEach( (element, index, array) => {
            let cbPromise;
            if(callback){
                cbPromise = callback(element, index, array);
                if(cbPromise){
                    promises.addCbPromise( cbPromise );
                }
            }
            
            if( --length === 0){
                promises.then( () => {
                    resolve();
                });
            }
            
        });
    });

};

function getPromises(){
    
    const list = [];
    
    return {
        then: then,
        addCbPromise: addCbPromise
    };

    function addCbPromise(promise){
        list.push(promise);   
    }
    
    function then(callback){
        if(list.length > 0){
            Promise.all(list).then(()=>callback());
        }
        else{
            callback();
        }
        
    }

}