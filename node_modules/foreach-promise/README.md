# foreach-promise
handle series of nested forEachs as one big promise

If you are tired of handling js asynchronous forEachs with counters and stuff, this little module will help you.

### Install

```javascript
npm install foreach-promise --save
```

### Usage

```javsascript
 const forEP = require('foreach-promise');
 
 forEP(array, callback) : returns a Promise that resolves when all the data is traversed
 callback can be a function which returns another forEP execution. (for foreach nesting)
 callback will be called with element, index, array arguments.
```



Let's say you're coding a function to generate names with nouns and adjectives, maybe you code something like the function below to do this.

```javascript
function createNames(nouns, adjectives){

    const namesList = [];

    return new Promise( (resolve, reject) => {
        let nounsLen = nouns.length;
        nouns.forEach(function(n){

            let adjLen = adjectives.length;
            adjectives.forEach(function(ad){

                namesList.push( ad + " " + n );
                
                //resolve this promise with the built namesList
                //the conditional below is to make sure my Promise only will resolve when all data is processed
                if(--nounsLen <= 0 && --adjLen <= 0){
                    resolve(namesList);
                }

            });
        });

    });

}
```

But you really dont wanna do this everytime you need some data manipulation like this.

And this can be a mess as the number of nested forEachs increases.

So, through this module, which I call: [foreach-promise](https://github.com/vmvini/foreach-promise/) , you can implement the function above like the code below:

```javascript
const forEP = require('foreach-promise');
function createNames(nouns, adjectives){

    const namesList = [];

    return new Promise( (resolve, reject) => {
        forEP(nouns, (n, index, array)=> {
            return forEP(adjectives, (ad, index, array)=>{ //for nested forEP, we should return a promise. and forEP returns that promise.
                namesList.push( ad + " " + n ); //here we do not return a promise, as we dont want more nested forEP
            });
        })
        .then(()=> resolve(namesList));
  });

}
```

For more examples, take a look at the tests within test folder.
