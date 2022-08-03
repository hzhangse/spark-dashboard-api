package main

import (
	"spark-api/controllers"
	"spark-api/utils"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	v1 := router.Group("/api/v1")
	{
		v1.GET("/sparkapp/:app", controllers.SparkAppControllerHandler())
		v1.GET("/sparkapp/", controllers.SparkAppControllerHandler())
	}

	router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"msg": "Not found"})
	})
	utils.Consume()
	router.Run(":8087")

}

// func fibonacci(c chan int, quit chan bool) {
// 	//x, y := 0, 1
// re:
// 	for {
// 		select {
// 		// case c <- x:
// 		// 	x, y = y, x+y
// 		case b := <-quit:
// 			fmt.Println(b)
// 			break re
// 		}
// 	}
// }
// func main() {
// 	c := make(chan int)
// 	quit := make(chan bool)
// 	//quit <- true
// 	go func() {
// 		// for i := 0; i < 10; i++ {
// 		// 	fmt.Println(<-c)
// 		// }
// 		quit <- true
// 	}()
// 	fibonacci(c, quit)
// }
