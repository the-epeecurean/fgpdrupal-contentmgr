const forEP = require('./../index');

describe("foreach-promise tests", () => {

    it("creates 6 combined names", (done) => {
        console.log("\ncreates 6 combined names");
        const names = [];
        forEP(['cat', 'bird'], (animal)=> {
            return forEP(['fat', 'old', 'lazy'], (adjective)=>{
                let name = adjective + " " + animal;
                names.push(name);
                console.log(name);
            });
        })
        .then(()=>{
            expect(names.length).toBe(6);
            done();
        });
    });


    it("creates an array with all possible combinations of this 5 digits [0,1,2,3,4], including repeated numbers", (done) => {
        console.log("\ncreates an array with all possible combinations of this 5 digits [0,1,2,3,4], including repeated numbers");
        const combinations = [];
        const digits = [0,1,2,3,4];

        forEP(digits, (d1)=>{
            return forEP(digits, (d2)=>{
                return forEP(digits, (d3)=>{
                    return forEP(digits, (d4)=>{
                        return forEP(digits, (d5)=>{
                            combinations.push([d1, d2, d3, d4, d5]);
                            console.log(d1 + "," + d2 + "," + d3 + "," + d4 + "," + d5);
                        });
                    });
                });
            });
        })
        .then(()=>{
            expect(combinations.length).toBe(5*5*5*5*5);
            done();
        });

    });

    it("creates an array with all possible combinations of this 5 digits [0,1,2,3,4] without repeated numbers", (done)=>{
        console.log("\ncreates an array with all possible combinations of this 5 digits [0,1,2,3,4] without repeated numbers");
        const combinations = [];
        const digits = [0,1,2,3,4];
        forEP(digits, (d1, i, array)=>{
            return forEP( array.filter(el=>(el!==d1)), (d2, i, array)=>{
                return forEP( array.filter(el=>(el!==d2)), (d3, i, array)=>{
                    return forEP( array.filter(el=>(el!==d3)), (d4, i, array)=>{
                        return forEP( array.filter(el=>(el!==d4)), (d5, i, array)=>{
                            combinations.push([d1, d2, d3, d4, d5]);
                            console.log(d1 + "," + d2 + "," + d3 + "," + d4 + "," + d5);
                        });
                    });
                });
            });
        })
        .then(()=>{
            expect(combinations.length).toBe(5*4*3*2);
            done();
        })
    });

});
